import 'dotenv/config';
import { randomBytes } from 'crypto';
import v8 from 'v8';
import vm from 'vm';
import Fastify from 'fastify';
import websocket from '@fastify/websocket';
import * as jwt from 'jsonwebtoken';
import { handleClientConnection } from './client-handler';
import { startRoomTick } from './input-batcher';
import { handlePeerConnection } from './peer-handler';
import { syncMasterClientList } from './sync-utils';
import { createPeerManager, peerRelayStats } from './peer-manager';
import { roomManager } from './room-manager';
import { voiceRelay } from './voice-relay';
import type WebSocket from 'ws';
import { wsToHttp, peerUrlFromApi } from './utils';
import path from 'path';
import { promises as fs } from 'fs';
import { MessageType, type NetworkInput } from './types';
import { encodeClientListUpdate } from './binary-protocol';
import type { PeerManager } from './peer-manager';
import { getCurrentFrame, queueInputForPeers, queueInputForClients, cleanupRoomTick } from './input-batcher';
import { initMeshAuth, requireMeshAuth, selfAuthHeaders, meshAuthMode } from './mesh-auth';

const NODE_NAME = process.env.NODE_NAME || 'Node';
const NODE_PORT = parseInt(process.env.NODE_PORT || '8001');
/**
 * This node's own HTTP API, for other nodes and for the central service.
 *
 * Distinct from NODE_PUBLIC_URL, which is where browsers connect and may point
 * at a tunnel. Anything that is not a browser needs to reach the node itself.
 */
const NODE_API_PUBLIC_URL = process.env.NODE_API_PUBLIC_URL || `http://localhost:${NODE_PORT}`;
const CENTRAL_SERVICE_URL = process.env.CENTRAL_SERVICE_URL || 'http://localhost:9001';
const NODE_PUBLIC_URL = process.env.NODE_PUBLIC_URL || `ws://localhost:${NODE_PORT}/ws`;
/**
 * The node token from the cloud portal, which registers this node under the
 * account that minted it. Optional against a central running with open
 * registration (local development); required by one running
 * NODE_REGISTRATION=token, which is what a public cloud does.
 */
const ARRR_NODE_TOKEN = (process.env.ARRR_NODE_TOKEN || process.env.NODE_TOKEN || '').trim();

// Ephemeral Node ID (reset on restart)
// This ensures that if we restart, we get a new ID, and the central service will replace the old one
// verified via the callback to our URL
let NODE_ID = process.env.NODE_ID || `node_${Date.now()}_${randomBytes(6).toString('hex')}`;

// Will be set after registration with central service (should match NODE_ID)
let registeredNodeId: string | null = null;

// Validation key received from central service during registration
// Used for JWT verification of clients connecting to this node
let nodeValidationKey: string | null = null;

// Export getter for validation key (used by client-handler for JWT verification)
export function getNodeValidationKey(): string | null {
  return nodeValidationKey;
}

/** This node's id as central knows it, falling back to the id it will claim. */
export function getNodeId(): string {
  return registeredNodeId || NODE_ID;
}

// Mesh credentials are verified against the same key, so the module that does
// the verifying needs to be able to read it. Accessors rather than values: both
// are null until registration completes.
initMeshAuth({ validationKey: getNodeValidationKey, nodeId: getNodeId });

// Track connected client count
let clientCount = 0;

// Bandwidth tracking
let bytesIn = 0;
let bytesOut = 0;
let lastBytesIn = 0;
let lastBytesOut = 0;
let bandwidthIn = 0;  // bytes per second
let bandwidthOut = 0; // bytes per second
const bandwidthHistory: { time: number; in: number; out: number }[] = [];
const MAX_HISTORY = 60; // Keep 60 seconds of history

// Update bandwidth every second
setInterval(() => {
  bandwidthIn = bytesIn - lastBytesIn;
  bandwidthOut = bytesOut - lastBytesOut;
  lastBytesIn = bytesIn;
  lastBytesOut = bytesOut;

  // Add to history
  bandwidthHistory.push({ time: Date.now(), in: bandwidthIn, out: bandwidthOut });
  if (bandwidthHistory.length > MAX_HISTORY) {
    bandwidthHistory.shift();
  }
}, 1000);


/**
 * Remove players who disconnected and never came back.
 *
 * Membership is deliberately kept when a client drops, so a reconnect resumes
 * rather than restarts - but nothing ever expired it, so a closed tab left a
 * player in the simulated world permanently. In a long-lived room every visitor
 * accumulates: standing still, still a target, still counted, and still costing
 * every client memory and simulation work. A room running for a quarter of an
 * hour was found with seven players in it and five clients connected.
 *
 * After the grace period the member is dropped and a `leave` goes out through
 * the normal input stream, so every client applies it at the same frame and
 * converges on their absence like any other event. The grace period is long
 * enough that an ordinary reconnect - a refresh, a flaky network - resumes
 * untouched.
 *
 * Which grace applies depends on what the client said on its way out. A socket
 * that simply died gets the long one, because it may be a network that is
 * coming back. A page that announced its own unload gets the short one: it is
 * closing or reloading, and both of those are answered within seconds. Two
 * minutes was the whole wait for anybody who quit a demo, and it is what a
 * player sees when they close their tab and stay on the leaderboard.
 */
const GHOST_GRACE_MS = Number(process.env.GHOST_GRACE_MS || 120_000);
/**
 * The grace for a member whose page told us it was unloading.
 *
 * The long grace is priced for a network that might come back: the SDK redials
 * with a backoff capped at five seconds, so two minutes is many attempts. An
 * unload is not that. The page is either closing, in which case nothing is
 * coming back, or reloading, in which case the same member rejoins as soon as
 * the new page has connected - a couple of seconds, and this is generous
 * against a slow one. Either way the answer arrives long before two minutes, so
 * waiting that long only leaves a body standing in the arena.
 */
const UNLOAD_GRACE_MS = Number(process.env.UNLOAD_GRACE_MS || 15_000);
/**
 * How often the sweep runs.
 *
 * The interval is added to whichever grace applies, so at thirty seconds it was
 * the larger half of the wait for anyone who quit. It costs one pass over the
 * rooms this node holds.
 */
const GHOST_SWEEP_MS = Number(process.env.GHOST_SWEEP_MS || 5_000);

function graceFor(member: { unloading?: boolean }): number {
  return member.unloading ? UNLOAD_GRACE_MS : GHOST_GRACE_MS;
}

function reapDepartedMembers() {
  const now = Date.now();
  for (const room of roomManager.getAllRooms()) {
    for (const member of roomManager.getMembers(room.id)) {
      if (member.status !== 'disconnected' || !member.disconnectedAt) continue;
      if (now - member.disconnectedAt < graceFor(member)) continue;

      /**
       * The player may be back already, through a different node.
       *
       * Membership is per node - it is held by whichever node the socket was
       * on - so a player who reconnects somewhere else becomes a member there
       * and leaves a disconnected one behind here. Expiring that record is
       * right; announcing it as a departure is not, because the announcement
       * names the *player*, and every client would then remove somebody who is
       * connected and playing. Measured: a refresh that landed on the other
       * node had the player deleted from every world fifteen seconds later.
       *
       * The room's client list spans every node, so it can answer "is this
       * player connected anywhere" where the members map cannot. If they are,
       * the stale record goes quietly and nothing is broadcast.
       */
      const connectedElsewhere = room.masterClientList
        && Array.from(room.masterClientList.values())
          .some((c) => c.metadata?.id === member.odId && c.clientId !== member.clientId);
      if (connectedElsewhere) {
        roomManager.removeMember(room.id, member.odId);
        console.log(
          `[REAP] dropped this node's stale membership for ${member.odId} in ${room.id}: ` +
          `they are connected through another node`
        );
        continue;
      }

      const leaveInput: NetworkInput = {
        id: `leave-${member.odId}-${now}`,
        clientId: member.clientId || member.odId,
        type: 'leave',
        data: { type: 'leave', clientId: member.clientId, user: member.odMetadata },
        seq: 0,
        // No frame: sendTick stamps the frame it is actually broadcast in.
        // Stamping here would claim a frame no client has received, and a late
        // joiner replaying that history would apply it a tick early and diverge.
      };

      /**
       * A replica sweeps its own members too, and hands the leave to the
       * authority to sequence.
       *
       * Membership lives on the node the client connected to, and only that
       * node ever learns that the socket dropped - the authority holds nothing
       * for a client of another node beyond an entry in the master client list.
       * So the sweep skipping every non-authority room meant nobody swept those
       * members at all: a player who closed their tab while connected to a
       * replica was never expired by anyone, and stayed in the world and on
       * every leaderboard for the life of the room. The only reason that is not
       * the common case is that central routes a room's clients to its
       * authority by default.
       */
      if (!room.isAuthority) {
        peerManager.broadcastToPeers({
          type: MessageType.RELAY_INPUT,
          payload: { roomId: room.id, input: leaveInput }
        });
        roomManager.removeMember(room.id, member.odId);
        console.log(
          `[REAP] ${member.odId} left ${room.id} after ` +
          `${Math.round((now - member.disconnectedAt) / 1000)}s disconnected ` +
          `(relayed to the authority)`
        );
        continue;
      }

      const seq = roomManager.addInput(room.id, leaveInput);
      if (!seq) continue;
      leaveInput.seq = seq;
      queueInputForPeers(room.id, leaveInput, peerManager);
      queueInputForClients(room.id, leaveInput, peerManager);
      roomManager.removeMember(room.id, member.odId);
      console.log(
        `[REAP] ${member.odId} left ${room.id} after ` +
        `${Math.round((now - member.disconnectedAt) / 1000)}s disconnected (seq=${seq})`
      );
    }
  }
}

setInterval(() => {
  try {
    reapDepartedMembers();
  } catch (err) {
    console.error('[REAP] sweep threw:', err);
  }
}, GHOST_SWEEP_MS).unref?.();

/**
 * Stay peered with the authority of every room this node only replicates.
 *
 * The peer link used to be dialled once, from the join handler, and never
 * again. A dial that failed for any reason left the replica permanently
 * stranded: it holds clients, it is not the authority, and with no peer it
 * never receives the input stream, so those clients sit connected and frozen
 * forever. Measured exactly that way - two clients at frames 12 and 32 while
 * the rest of their room ran past 1700, and both nodes reporting zero peers.
 *
 * The failure that exposed it is ordinary and will happen again: the dial
 * raced node registration, so the authority's own API address had not been
 * published yet, the URL fell back to one that does not answer, and nothing
 * ever tried a second time. Rather than special-case that race, this states
 * the invariant - a replica is peered with its authority - and re-checks it.
 */
const PEER_ENSURE_MS = Number(process.env.PEER_ENSURE_MS || 10_000);
/**
 * How often to re-ask central who owns a room we are already peered for.
 *
 * The first version of this asked about every replicated room on every sweep,
 * which is two HTTP calls per room per ten seconds for something that almost
 * never changes - fine for three rooms and a poor idea for a thousand. Once we
 * hold a live peer link to a room's authority there is nothing to do, so the
 * answer is cached and only re-checked occasionally, which bounds how long a
 * moved authority can go unnoticed to about a minute.
 */
const PEER_REFRESH_SWEEPS = Number(process.env.PEER_REFRESH_SWEEPS || 6);
const knownAuthority = new Map<string, string>();
let peerSweepCount = 0;

async function ensureReplicaPeers() {
  const replicaRooms = roomManager.getAllRooms().filter((r) => !r.isAuthority);
  if (replicaRooms.length === 0) return;
  const refresh = (peerSweepCount++ % PEER_REFRESH_SWEEPS) === 0;

  // Forget rooms this node no longer replicates, so the map cannot outlive them.
  if (refresh) {
    const live = new Set(replicaRooms.map((r) => r.id));
    for (const id of knownAuthority.keys()) if (!live.has(id)) knownAuthority.delete(id);
  }

  const connected = new Set(peerManager.getPeers().filter((p) => p.isConnected).map((p) => p.id));
  const centralServiceUrl = process.env.CENTRAL_SERVICE_URL || 'http://localhost:9001';

  let nodes: any[] | null = null;
  for (const room of replicaRooms) {
    try {
      // Already peered with the authority we last saw for this room, and not
      // due a re-check: nothing to ask anybody.
      const cached = knownAuthority.get(room.id);
      if (!refresh && cached && connected.has(cached)) continue;

      // Authenticated, because this is also where the credential for dialling
      // the authority comes from. Central only mints one for a caller that
      // proves it is a registered node.
      const roomRes = await fetch(`${centralServiceUrl}/api/rooms/${room.id}`,
        { headers: selfAuthHeaders(), signal: AbortSignal.timeout(3000) });
      if (!roomRes.ok) continue;
      const roomData = await roomRes.json() as any;
      const authorityId = roomData?.authorityNodeId;
      if (!authorityId) continue;
      knownAuthority.set(room.id, authorityId);

      // Central says this node owns the room, so own it.
      //
      // Authority is pushed to the promoted node by an HTTP call from central
      // when a room is assigned, and that is the only thing that ever set it.
      // If the call fails, or the reassignment happens without one - clients
      // reconnecting straight to a node that already holds the room, which is
      // what they do when their own node dies - then central's answer and the
      // node's own flag disagree, and nothing ever revisits it.
      //
      // Watched live: node2 was killed while it was the authority for three
      // rooms, its clients reconnected to node1, and central named node1 the
      // authority for all three. node1 went on holding them as replicas. The
      // rooms kept ticking - the tick loop does not consult the flag - so
      // everything looked healthy and nobody was in charge, which is the state
      // that produces two tick streams the moment the old authority comes back.
      // The clients recorded 1,144 duplicate ticks between them.
      //
      // Central stays the source of truth: this only adopts what central
      // already says, so it cannot create a second authority.
      if (authorityId === NODE_ID) {
        if (roomManager.promoteToAuthority(room.id)) {
          console.log(`[PEER] Central says this node is the authority for ${room.id} `
            + 'but it was held as a replica - promoting it');
        }
        continue;
      }
      if (connected.has(authorityId)) continue;

      // Fetched once per sweep, and only when something actually needs dialling.
      if (!nodes) {
        const statsRes = await fetch(`${centralServiceUrl}/api/dashboard/stats`,
          { headers: selfAuthHeaders(), signal: AbortSignal.timeout(3000) });
        if (!statsRes.ok) return;
        nodes = ((await statsRes.json()) as any).nodes || [];
      }
      const authority = nodes!.find((n: any) => n.id === authorityId);
      if (!authority) continue;

      const url = authority.apiUrl
        ? peerUrlFromApi(authority.apiUrl)
        : authority.connectionUrl;
      console.log(`[PEER] Room ${room.id} is replicated here but not peered with its authority ${authorityId} - dialling ${url}`);
      const ok = await peerManager.connectToPeer(authorityId, url, roomData?.peerToken);
      if (ok) connected.add(authorityId);
    } catch (err) {
      // Said out loud, not swallowed. A silent catch here would hide the only
      // evidence that the sweep ran at all - which it did, on the first
      // attempt at this, and I spent a restart wondering why nothing happened.
      console.log(`[PEER] Could not ensure a peer for ${room.id}: ${(err as Error).message}`);
    }
  }
}

setInterval(() => { void ensureReplicaPeers(); }, PEER_ENSURE_MS).unref?.();

/**
 * Tell the replicas who is in the room, on a timer as well as on events.
 *
 * The master client list was only ever pushed when it changed. A join or leave
 * that happened while the peer link was down was therefore missed for good:
 * nothing re-sent it, so the replica kept a membership list that was wrong from
 * that moment until the room ended.
 *
 * That is not a rare state. Peer links are dialled at startup and a dial can
 * lose the race with the peer's own routes coming up - observed exactly once in
 * an ordinary session, as `Peer connection error ... Unexpected server response:
 * 404`, after which the link established itself and everything else recovered.
 * Ticks flowed, hashes relayed both ways, and the membership stayed wrong.
 *
 * What that costs is not cosmetic. A replica's list decides which joins its
 * clients are given when they catch up, so a client on that node builds its
 * world from the wrong roster and disagrees with the room from its first frame.
 * Measured live: a bot on the replica holding a roster of [bot1, bot2,
 * latecomer] - a client that had left long before - against the room's [bot1,
 * bot2, bot3], with 18,655 disagreements and 187 resyncs that could not fix it,
 * because a resync repairs the world and not the roster it will be rebuilt
 * from.
 *
 * Re-sending is idempotent - the receiver replaces its list outright - so this
 * needs no change detection and no acknowledgement. It is a few entries per
 * room per interval, and it means any missed event costs one interval rather
 * than the life of the room.
 */
const MEMBERSHIP_SYNC_MS = Number(process.env.MEMBERSHIP_SYNC_MS || 15_000);
/** Rooms re-synced on the timer. Climbs while nothing is changing, by design. */
let membershipSyncs = 0;

setInterval(() => {
  try {
    for (const room of roomManager.getAllRooms()) {
      if (!room.isAuthority || !room.masterClientList?.size) continue;
      syncMasterClientList(room.id, room, peerManager, { peersOnly: true });
      membershipSyncs++;
    }
  } catch (err) {
    console.error('[MEMBERSHIP] periodic sync threw:', err);
  }
}, MEMBERSHIP_SYNC_MS).unref?.();

/**
 * Last resort. The central service has had these for a while; the node, which
 * is the process actually holding every room and every connection, did not.
 *
 * That asymmetry is exactly backwards. A control plane that dies strands the
 * clients on it; a node that dies takes the games with it, mid-frame. And it
 * did: an input too large for the tick encoding threw inside a room's tick
 * timer, nothing caught it, and Node's default for an uncaught exception in a
 * timer callback ended the process. Every room on that node went with it, from
 * one client sending one message.
 *
 * The specific hole is closed at three levels now, but "no code path anywhere
 * in this process may ever throw somewhere unguarded" is not a property anyone
 * can maintain by hand. Staying up half-broken beats vanishing.
 */
process.on('unhandledRejection', (reason) => {
  const message = reason instanceof Error ? reason.message : String(reason);
  console.error(`[UNHANDLED] rejection: ${message}`);
  if (reason instanceof Error && reason.stack) console.error(reason.stack);
});

process.on('uncaughtException', (err) => {
  console.error(`[UNCAUGHT] ${err.message}`);
  if (err.stack) console.error(err.stack);
});

const app = Fastify({ logger: true });
const peerManager = createPeerManager(NODE_ID);

app.register(websocket);

// WebSocket endpoint for clients
app.register(async (fastify) => {
  fastify.get('/ws', { websocket: true }, (socket: WebSocket, request) => {
    markAlive(socket);
    try {
      // Extract JWT token from query parameter
      const token = (request.query as any).token;

      if (!token) {
        console.error('Client connection rejected: No token provided');
        socket.close(4001, 'Authentication required');
        return;
      }

      // Verify JWT token using node's validation key
      const validationKey = getNodeValidationKey();
      if (!validationKey) {
        console.error('Client connection rejected: Node validation key not available (has node registered with central?)');
        socket.close(4000, 'Server not ready');
        return;
      }

      let decoded;
      try {
        decoded = jwt.verify(token, validationKey) as {
          clientId: string;
          clientMetadata?: Record<string, any>;
          roomId: string;
          isMuted?: boolean;
          readOnly?: boolean;
        };
      } catch (err: any) {
        console.error(`Client connection rejected: Invalid token - ${err.message}`);
        console.error(`  Token (first 50 chars): ${token.substring(0, 50)}...`);
        console.error(`  ValidationKey (first 10 chars): ${validationKey.substring(0, 10)}...`);
        socket.close(4001, 'Invalid token');
        return;
      }

      clientCount++;
      console.log(`Client authenticated: ${decoded.clientId} for room ${decoded.roomId} (total: ${clientCount})`);

      // Track incoming bytes
      socket.on('message', (data: Buffer) => {
        bytesIn += data.length;
      });

      // Wrap send to track outgoing bytes with message type breakdown
      const originalSend = socket.send.bind(socket);
      socket.send = (data: any, ...args: any[]) => {
        let size = 0;
        let msgType = 'unknown';
        if (typeof data === 'string') {
          size = data.length;
          msgType = 'string';
        } else if (Buffer.isBuffer(data)) {
          size = data.length;
          // Decode message type from first byte
          const typeMap: Record<number, string> = { 0x01: 'TICK', 0x02: 'INITIAL_STATE', 0x03: 'ROOM_CREATED', 0x04: 'ROOM_JOINED', 0x05: 'ROOM_LEFT', 0x06: 'ERROR', 0x07: 'CLIENT_LIST', 0xF0: 'SNAPSHOT' };
          msgType = typeMap[data[0]] || `0x${data[0].toString(16)}`;
        }
        bytesOut += size;
        // Log any message over 1KB to track large messages
        if (size > 1024) {
          console.log(`[LARGE-MSG] ${msgType} ${size}B`);
        }
        return originalSend(data, ...args);
      };

      socket.on('close', () => {
        clientCount--;
        console.log(`Client disconnected (total: ${clientCount})`);
      });

      handleClientConnection(socket, peerManager, NODE_ID, decoded);
    } catch (err: any) {
      console.error(`WebSocket handler error: ${err.message}`, err.stack);
      socket.close(4500, 'Internal server error');
    }
  });
});

/**
 * Close connections that have stopped answering.
 *
 * Nothing did this. There was no ping, no pong and no timeout on any socket
 * the node holds, so a connection only ever went away when the peer closed it
 * politely or TCP eventually gave up - which for a half-open connection can be
 * hours, and for one that is merely unreadable is never.
 *
 * That is the ordinary way a mobile client leaves: the network drops without a
 * FIN. Until TCP notices, the node keeps that client in the room's roster,
 * keeps broadcasting every tick to it, and keeps the bytes it cannot deliver in
 * this process's memory - nothing checks bufferedAmount either. The client is
 * gone and is still being paid for.
 *
 * A ping every interval, terminate if the previous one was never answered, so a
 * dead connection costs at most two intervals. The `ws` library answers pings
 * automatically, so every well-behaved client and peer already satisfies this
 * without changing anything on their side. Terminating runs the normal close
 * path, which is what puts the member into its reconnect grace period rather
 * than removing them outright.
 */
const WS_HEARTBEAT_MS = Number(process.env.WS_HEARTBEAT_MS || 30_000);
let socketsTerminated = 0;

function markAlive(socket: any): void {
  socket.isAlive = true;
  socket.on('pong', () => { socket.isAlive = true; });
}

setInterval(() => {
  const wss = (app as any).websocketServer;
  if (!wss?.clients) return;
  for (const socket of wss.clients) {
    const s = socket as any;
    if (s.isAlive === false) {
      socketsTerminated++;
      console.warn('[WS] terminating a connection that answered no ping in '
        + `${Math.round(WS_HEARTBEAT_MS / 1000)}s`);
      try { socket.terminate(); } catch { /* already gone */ }
      continue;
    }
    s.isAlive = false;
    try { socket.ping(); } catch { /* closing */ }
  }
}, WS_HEARTBEAT_MS).unref?.();

// WebSocket endpoint for peer nodes
app.register(async (fastify) => {
  fastify.get('/ws/peer', { websocket: true }, (socket, request) => {
    markAlive(socket);
    // Track incoming bytes from peers
    socket.on('message', (data: Buffer) => {
      bytesIn += data.length;
    });

    // Wrap send to track outgoing bytes to peers
    const originalSend = socket.send.bind(socket);
    socket.send = (data: any, ...args: any[]) => {
      if (typeof data === 'string') {
        bytesOut += data.length;
      } else if (Buffer.isBuffer(data)) {
        bytesOut += data.length;
      }
      return originalSend(data, ...args);
    };

    handlePeerConnection(socket, peerManager, NODE_ID);
  });
});

// Health check endpoint
/**
 * Force a full collection and report what is still held.
 *
 * Heap usage sampled whenever you happen to ask is the wrong number for
 * answering "is this leaking". It rises between collections whether or not
 * anything is being retained, so a rising sample proves nothing - and the low
 * points only tell you a collection happened to land near a sample. Fifteen
 * minutes of 30-second samples here showed the floor climbing from 42MB to
 * 54MB, which is either a leak or an artefact of when GCs fell, and no amount
 * of further sampling separates the two.
 *
 * --expose-gc would normally be needed at startup, which means a flag on a
 * process that is usually already running by the time the question comes up.
 * Turning the flag on from inside and compiling a reference to it avoids that.
 * Deliberately not part of /health: a full collection pauses the process, which
 * is not something a liveness probe should do on a whim.
 */
let forceGc: (() => void) | null = null;
function collectGarbage(): boolean {
  if (!forceGc) {
    try {
      v8.setFlagsFromString('--expose_gc');
      forceGc = vm.runInNewContext('gc') as () => void;
    } catch { return false; }
    finally { try { v8.setFlagsFromString('--no-expose_gc'); } catch { /* best effort */ } }
  }
  try { forceGc(); return true; } catch { return false; }
}

/**
 * Where allocations are coming from, by call site.
 *
 * Counting the structures a leak might be in only works if you guessed the
 * right structures. Retained memory on this node climbs about 100MB an hour
 * after a forced collection while every counter reported here - rooms, members,
 * client lists, collector frames, input backlogs - stays flat. So the answer is
 * somewhere nobody thought to count, and the only honest way to find that is to
 * ask the heap rather than the author.
 *
 * Samples allocation stacks for a few seconds and returns the heaviest, which
 * names the function doing the allocating instead of the object being kept.
 * Deliberately off the ordinary health route: sampling costs something, and a
 * liveness probe should not.
 */
/**
 * Write a heap snapshot to disk and say where it went.
 *
 * Counting structures answers "is it one of these", and the answer has been no.
 * Sampling allocations answers "what allocates", which is churn rather than
 * retention. Neither names what is actually being kept, and guessing which
 * container to count next is not a method - two rounds of it produced only
 * eliminations.
 *
 * A snapshot names it. Every object in the heap with its type, its size and
 * what refers to it; diff two of them and the thing that grew is simply listed.
 * Off the ordinary health route because writing one pauses the process for as
 * long as it takes to serialise the heap.
 */
app.get<{ Querystring: { path?: string } }>('/health/heapdump', async (request) => {
  const v8mod = await import('node:v8');
  const dir = request.query.path || process.env.HEAPDUMP_DIR || '/tmp';
  const file = `${dir}/node-${NODE_ID.slice(-8)}-${Math.round(process.uptime())}s.heapsnapshot`;
  const written = v8mod.writeHeapSnapshot(file);
  const { statSync } = await import('node:fs');
  return {
    file: written,
    bytes: statSync(written).size,
    uptimeSeconds: Math.round(process.uptime()),
    heapUsedMb: Math.round(process.memoryUsage().heapUsed / 1048576),
  };
});

app.get<{ Querystring: { seconds?: string } }>('/health/allocations', async (request) => {
  const seconds = Math.min(60, Math.max(1, Number(request.query.seconds) || 15));
  const { Session } = await import('node:inspector');
  const session = new Session();
  session.connect();

  const post = (method: string, params?: any) => new Promise<any>((resolve, reject) => {
    (session as any).post(method, params, (err: Error | null, result: any) =>
      err ? reject(err) : resolve(result));
  });

  try {
    await post('HeapProfiler.enable');
    await post('HeapProfiler.startSampling', { samplingInterval: 16384 });
    await new Promise((r) => setTimeout(r, seconds * 1000));
    const { profile } = await post('HeapProfiler.stopSampling');

    // Fold the tree into a flat list keyed by call site, keeping self size -
    // the bytes allocated by that function itself rather than by its callees.
    const bySite = new Map<string, number>();
    const walk = (node: any) => {
      const f = node.callFrame || {};
      const where = `${f.functionName || '(anonymous)'} ${String(f.url || '').split('/').slice(-1)[0]}:${f.lineNumber ?? '?'}`;
      if (node.selfSize > 0) bySite.set(where, (bySite.get(where) || 0) + node.selfSize);
      for (const child of node.children || []) walk(child);
    };
    walk(profile.head);

    const top = [...bySite.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 25)
      .map(([site, bytes]) => ({ site, kb: Math.round(bytes / 1024) }));

    return { sampledSeconds: seconds, top };
  } finally {
    try { await post('HeapProfiler.disable'); } catch { /* best effort */ }
    session.disconnect();
  }
});

app.get('/health/retained', async () => {
  const collected = collectGarbage();
  const mem = process.memoryUsage();
  const mb = (n: number) => Math.round(n / 1048576);
  return {
    collected,
    // With a collection just done, this is memory genuinely still reachable.
    // Growth here across samples is a leak; growth in /health may be nothing.
    heapUsedMb: mb(mem.heapUsed),
    heapTotalMb: mb(mem.heapTotal),
    rssMb: mb(mem.rss),
    externalMb: mb(mem.external),
    rooms: roomManager.getAllRooms().length,
    inputs: roomManager.getAllRooms().reduce((t, r) => t + (r.inputs?.length ?? 0), 0),
    uptimeSeconds: Math.round(process.uptime()),
    // Which part of the heap is growing, which narrows what is in it before
    // anyone goes looking. Many small objects land in old_space; large strings
    // and buffers go to large_object_space; compiled code to code_space. The
    // structure counters said the leak is nowhere they can see, so the next
    // useful question is what shape the retained thing is.
    spaces: v8.getHeapSpaceStatistics()
      .filter((s) => s.space_used_size > 1024 * 1024)
      .map((s) => ({ space: s.space_name, usedMb: Math.round(s.space_used_size / 1048576) })),
    // Sizes of everything that lives as long as the node does. Retained memory
    // climbing on a steady workload has to be one of these growing, and
    // narrowing it down by reasoning has a poor record - so count them and let
    // the numbers say which.
    // Which way relayed state hashes are actually travelling. The vote totals
    // alone could not distinguish a send that never happened from a receive
    // that was dropped.
    hashRelay: {
      sent: peerRelayStats.sent,
      received: peerRelayStats.received,
      noPeers: peerRelayStats.noPeers,
    },
    // And which links exist to travel over. A node showing hundreds of
    // thousands sent and its peer showing none received is either a dropped
    // message, an uncounted one, or a socket that is registered as connected
    // and is not open - and those were indistinguishable from outside, because
    // nothing reported the links at all. readyState is the socket's own
    // opinion, which is the one that decides whether a send arrives.
    peers: peerManager.getPeers().map((p) => ({
      id: p.id,
      isConnected: p.isConnected,
      readyState: (p.socket as any)?.readyState ?? null,
    })),
    // Re-sends of the room membership that were not prompted by a change. A
    // flat count here means a replica that has missed one will never be told.
    membershipSyncs,
    structures: roomManager.debugSizes(),
  };
});

app.get('/health', async () => {
  // Memory alongside liveness. "Is it up" is the easy half; a node that is up
  // and slowly growing is the failure that actually takes a session down, and
  // without heap numbers here the only signal is resident size - which includes
  // memory V8 has not returned to the OS and so cannot distinguish a leak from
  // an allocator holding on to pages.
  const mem = process.memoryUsage();
  const mb = (n: number) => Math.round(n / 1048576);
  return {
    status: 'ok',
    nodeId: NODE_ID,
    // Which checkout this node was started from.
    //
    // The e2e cluster reuses whatever is already listening on its ports, and
    // two worktrees of this repo on one machine use the same ports - so a suite
    // can run its specs against a node built from entirely different source and
    // report the result as its own. Saying where this came from is what lets
    // the other side notice.
    checkout: process.cwd(),
    uptimeSeconds: Math.round(process.uptime()),
    rooms: roomManager.getAllRooms().length,
    // Voice: how many people are in earshot of the relay, and how much of it
    // is moving. Packets out over packets in is the fan-out actually paid.
    voice: voiceRelay.stats(),
    memory: {
      rssMb: mb(mem.rss),
      heapUsedMb: mb(mem.heapUsed),
      heapTotalMb: mb(mem.heapTotal),
      externalMb: mb(mem.external),
      arrayBuffersMb: mb(mem.arrayBuffers),
    },
    // Where a growing node's memory actually goes. Both of these have been the
    // cause of an out-of-memory kill here before - a room that kept every input
    // it had ever seen, and a chat room that serialised its entire history into
    // a snapshot once a second. Both are bounded now, and neither bound was
    // observable from outside the process: this endpoint would report a
    // perfectly healthy 3 rooms while one of them held a hundred thousand
    // inputs. Per-room detail means the next one can be seen coming instead of
    // being diagnosed from a corpse.
    roomDetail: roomManager.getAllRooms().map((room) => ({
      id: room.id,
      clients: room.clients?.size ?? 0,
      inputs: room.inputs?.length ?? 0,
      // Membership, separately from connections. A room is kept alive by either,
      // and a room that outlives its clients is being held up by members - which
      // is invisible if only client counts are reported.
      members: room.members?.size ?? 0,
      membersConnected: roomManager.getMembers(room.id).filter((m) => m.status === 'connected').length,
      snapshotBytes: room.snapshotSize ?? 0,
      snapshotFrame: room.snapshot?.frame ?? null,
      isAuthority: !!room.isAuthority,
    })),
  };
});

/**
 * Take authority for a room this node is already hosting as a replica.
 *
 * Called by central when it reassigns a room whose authority went away. Until
 * this existed, central told the replicas who the new authority was but never
 * told the new authority itself - which is fine for a room being created, since
 * the first client's CREATE_ROOM establishes the role, and wrong for a room
 * being reassigned, where the promoted node already has the room and never
 * calls createRoom again.
 *
 * The effect of missing it was a room with no authority anywhere: the node
 * holding all the players kept it as a replica, and a replica does not tick, so
 * the world stopped while every player sat there connected and reporting fine.
 *
 * Nothing to do if the room is not here - a client joining will create it - and
 * nothing to do if this node already holds authority.
 */
app.post<{ Body: { roomId: string } }>(
  '/api/cluster/authority',
  async (request, reply) => {
    const { roomId } = request.body;
    if (!roomId) return reply.code(400).send({ error: 'roomId is required' });
    // Seizing authority for a room is the most valuable thing on this surface:
    // the authority sequences every input and its snapshot is what late joiners
    // restore from. Anyone who could reach this port could take it.
    if (!requireMeshAuth(request, reply, 'authority', `authority claim for ${roomId}`, roomId)) return;

    const promoted = roomManager.promoteToAuthority(roomId);
    if (promoted) {
      // A replica follows someone else's ticks; an authority produces its own,
      // so the loop has to be running or the promotion changes nothing.
      startRoomTick(roomId, peerManager);
      console.log(`[AUTHORITY] Took authority for room ${roomId}`);
    }
    return { success: true, promoted };
  }
);

// Cluster connection endpoint (called by Central Service)
app.post<{ Body: { peerId: string; peerUrl: string; roomId?: string; peerToken?: string } }>(
  '/api/cluster/connect',
  async (request, reply) => {
    const { peerId, peerUrl, roomId, peerToken } = request.body;

    // Unguarded, this makes the node dial any URL the caller names and treat
    // whatever answers as a peer - both an outbound request on demand and a way
    // to introduce a peer of the attacker's choosing into the room's replication.
    if (!requireMeshAuth(request, reply, 'cluster-connect', `cluster connect to ${peerId}`)) return;

    console.log(`Received cluster connect request: connect to ${peerId} at ${peerUrl} ${roomId ? `for room ${roomId}` : ''}`);

    if (peerId === (registeredNodeId || NODE_ID)) {
      return { success: true, message: 'Cannot connect to self' };
    }

    // Connect to the peer
    // Central mints this for the node being dialled, not for us: we present it
    // unaltered and that node verifies it with its own key.
    const connected = await peerManager.connectToPeer(peerId, peerUrl, peerToken);

    if (connected && roomId) {
      // Optional: Could pre-provision room or log context
      console.log(`Successfully connected to peer ${peerId} for room ${roomId}`);
    }

    return { success: connected };
  }
);

// Stats endpoint for dashboard
app.get('/api/stats', async () => {
  const rooms = roomManager.getAllRooms();
  const peers = peerManager.getPeers();

  return {
    nodeId: registeredNodeId || NODE_ID,
    nodeName: NODE_NAME,
    clientCount,
    peerCount: peers.filter(p => p.isConnected).length,
    // Who they are, not just how many. A count cannot answer the question that
    // actually matters about this list - whether this node is in it - and a
    // node that has itself as a peer delivers every broadcast back to itself,
    // demotes itself from its own tick, and silently drops every join that
    // arrives while it flaps. That cost four hours to find from the outside,
    // because from the outside it looks like a room whose roster stopped
    // growing while every client agrees on the world.
    peers: peers.filter(p => p.isConnected).map(p => p.id),
    bandwidth: {
      in: bandwidthIn,
      out: bandwidthOut,
      totalIn: bytesIn,
      totalOut: bytesOut,
      history: bandwidthHistory
    },
    rooms: rooms.map(r => ({
      id: r.id,
      isAuthority: r.isAuthority,
      clientCount: r.clients.size,
      snapshotSeq: r.snapshot?.seq || 0,
      snapshotHash: r.snapshotHash ? r.snapshotHash.substring(0, 8) : 'none',
      snapshotSize: r.snapshotSize || 0,
      inputCount: r.inputs?.length || 0,
      lastInputSeq: r.inputs?.length > 0 ? r.inputs[r.inputs.length - 1]?.seq : 0,
      syncedClients: roomManager.getSyncedClientCount(r.id),
      clients: roomManager.getRoomSyncStatus(r.id)
    }))
  };
});

// Debug: Get room input count
/**
 * What rooms this node is holding, and why each is still here.
 *
 * There was no way to ask. Room count came from /health as a single number, so
 * when rooms accumulated the only available diagnosis was guesswork - and a
 * reaper that worked in isolation but not in the wild could not be told apart
 * from one that was never running.
 */
app.get('/api/rooms', async () => {
  const now = Date.now();
  const rooms = roomManager.getAllRooms().map((room) => {
    const members = roomManager.getMembers(room.id);
    return {
      id: room.id,
      isAuthority: room.isAuthority,
      clients: room.clients.size,
      members: members.length,
      disconnectedMembers: members.filter(m => m.status === 'disconnected').length,
      oldestDisconnectSeconds: members
        .filter(m => m.disconnectedAt)
        .reduce((oldest, m) => Math.max(oldest, Math.round((now - m.disconnectedAt!) / 1000)), 0),
      emptyForSeconds: room.emptySince ? Math.round((now - room.emptySince) / 1000) : null,
      inputs: room.inputs.length,
      snapshotFrame: room.snapshotFrame ?? null,
    };
  });
  return {
    count: rooms.length,
    idle: rooms.filter(r => r.clients === 0).length,
    rooms: rooms.sort((a, b) => a.clients - b.clients),
  };
});

app.get<{ Params: { roomId: string } }>('/api/rooms/:roomId/debug', async (request, reply) => {
  const { roomId } = request.params;
  const room = roomManager.getRoom(roomId);

  if (!room) {
    return reply.code(404).send({ error: 'Room not found' });
  }

  return {
    id: room.id,
    isAuthority: room.isAuthority,
    inputCount: room.inputs?.length || 0,
    lastSeq: room.inputs?.length > 0 ? room.inputs[room.inputs.length - 1]?.seq : 0,
    clientCount: room.clients.size,
    masterClientListCount: room.masterClientList?.size || 0,
    /**
     * Membership, which outlives connections and is what the ghost sweep acts
     * on. Nothing exposed it, so "is this player still a member, and how long
     * until they expire" could only be answered by reading the node's log -
     * which is exactly the question asked whenever somebody who left is still
     * in the world.
     */
    members: roomManager.getMembers(roomId).map((m) => ({
      odId: m.odId,
      status: m.status,
      unloading: Boolean(m.unloading),
      disconnectedForMs: m.disconnectedAt ? Date.now() - m.disconnectedAt : null,
    })),
    // What this room's consensus is actually built from. Node-wide totals
    // cannot answer a question about one room while other rooms are running.
    collector: roomManager.collectorSizes(roomId),
  };
});

// Delete a room from this node (called by central service during cleanup)
app.delete<{ Params: { roomId: string } }>('/api/rooms/:roomId', async (request, reply) => {
  const { roomId } = request.params;

  if (!requireMeshAuth(request, reply, 'room-delete', `delete of room ${roomId}`, roomId)) return;

  const deleted = roomManager.deleteRoom(roomId);
  if (deleted) {
    cleanupRoomTick(roomId);
  }

  return { success: true, deleted, roomId };
});

// Get master client list for a room (authority only)
app.get<{ Params: { roomId: string } }>('/api/rooms/:roomId/clients', async (request, reply) => {
  const { roomId } = request.params;
  const room = roomManager.getRoom(roomId);

  if (!room) {
    return reply.code(404).send({ error: 'Room not found' });
  }

  if (!room.isAuthority) {
    return reply.code(403).send({ error: 'Not authority for this room' });
  }

  const masterList = roomManager.getMasterClientList(roomId);
  if (!masterList) {
    return [];
  }

  return Array.from(masterList.values());
});

// Update client fields (isMuted, etc.) in a room
app.post<{ Params: { roomId: string }; Body: { clients: Array<{ clientId: string; isMuted?: boolean }> } }>(
  '/api/rooms/:roomId/clients',
  async (request, reply) => {
    const { roomId } = request.params;
    const { clients: clientUpdates } = request.body;

    const authHeader = request.headers['authorization'];
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return reply.code(401).send({ error: 'Missing or invalid token' });
    }

    const token = authHeader.split(' ')[1];
    const validationKey = getNodeValidationKey();

    if (!validationKey) {
      return reply.code(500).send({ error: 'Node not ready' });
    }

    try {
      const decoded = jwt.verify(token, validationKey) as any;
      if (!decoded.allowClientFieldUpdates) {
        return reply.code(403).send({ error: 'Token does not allow client field updates' });
      }
    } catch (err) {
      return reply.code(401).send({ error: 'Invalid token' });
    }

    const room = roomManager.getRoom(roomId);
    if (!room) {
      return reply.code(404).send({ error: 'Room not found' });
    }

    if (!room.isAuthority) {
      return reply.code(403).send({ error: 'Not authority for this room' });
    }

    if (!room.masterClientList) {
      room.masterClientList = new Map();
    }

    let updatedCount = 0;
    for (const update of clientUpdates) {
      const { clientId, isMuted } = update;
      const masterEntry = room.masterClientList.get(clientId);

      if (masterEntry) {
        if (isMuted !== undefined) {
          masterEntry.isMuted = isMuted;

          // Also update local client if it exists on this node
          const localClient = room.clients.get(clientId);
          if (localClient) {
            localClient.isMuted = isMuted;
          }
        }
        updatedCount++;
      }
    }

    if (updatedCount > 0) {
      // Broadcast updated master client list to all replicas and local clients
      syncMasterClientList(roomId, room, peerManager);
    }

    return { success: true, updatedCount };
  }
);

// Get room state (used by other nodes to sync)
/**
 * Optional static hosting for the example/test pages, off unless PLAY_DIR is set.
 *
 * This exists for environments where only the node's own port is reachable -
 * container port forwarding, remote devboxes, tunnels - so the pages can be
 * served from the same origin the client already has to reach for its
 * WebSocket, instead of needing a second forwarded port for a static server.
 *
 * Never enabled by default, and the directory is whatever the operator points
 * it at, so production serves nothing unless deliberately configured to.
 */
const PLAY_DIR = process.env.PLAY_DIR;
if (PLAY_DIR) {
  const playRoot = path.resolve(PLAY_DIR);
  const sdkRoot = path.resolve(PLAY_DIR, '..', '..', 'sdk', 'dist');
  const MIME: Record<string, string> = {
    '.html': 'text/html; charset=utf-8',
    '.js': 'text/javascript; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.map': 'application/json; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.svg': 'image/svg+xml',
  };

  console.log(`[PLAY] Serving pages from ${playRoot} at /play (and /play/sdk from ${sdkRoot})`);

  app.get<{ Params: { '*': string } }>('/play/*', async (request, reply) => {
    const rel = request.params['*'] || 'index.html';
    const isSdk = rel.startsWith('sdk/');
    const root = isSdk ? sdkRoot : playRoot;
    const file = path.resolve(root, isSdk ? rel.slice(4) : rel);

    // Contain the resolved path inside its root - the request path is attacker
    // controlled and `..` would otherwise escape.
    if (file !== root && !file.startsWith(root + path.sep)) {
      return reply.code(403).send({ error: 'forbidden' });
    }

    try {
      const body = await fs.readFile(file);
      return reply.type(MIME[path.extname(file)] || 'application/octet-stream').send(body);
    } catch {
      return reply.code(404).send({ error: `not found: ${rel}` });
    }
  });

  app.get('/play', async (_request, reply) => reply.redirect('/play/index.html'));
  // Convenience: the bare host is what people actually type.
  app.get('/', async (_request, reply) => reply.redirect('/play/index.html'));
}

/** Desyncs clients have reported for a room, newest last. Debugging aid. */
app.get<{ Params: { roomId: string } }>('/api/rooms/:roomId/desyncs', async (request, reply) => {
  const { roomId } = request.params;
  if (!roomManager.getRoom(roomId)) {
    return reply.code(404).send({ error: 'Room not found' });
  }
  const reports = roomManager.getDesyncReports(roomId);
  return { roomId, count: reports.length, reports };
});

app.get<{ Params: { roomId: string } }>('/api/rooms/:roomId/state', async (request, reply) => {
  const { roomId } = request.params;
  const room = roomManager.getRoom(roomId);

  if (!room) {
    return reply.code(404).send({ error: 'Room not found' });
  }

  const currentFrame = getCurrentFrame(roomId);

  // Serve only inputs that have actually been broadcast.
  //
  // This used to back-fill `frame = currentFrame + 1` onto every unstamped input
  // in room.inputs. Two things were wrong with that. It made a GET mutate
  // authoritative state, so anything polling this endpoint - a replica joining, a
  // dashboard, a health check - silently rewrote the room's history. And the
  // value was a guess: an input is stamped with the frame it is actually
  // broadcast in, which is only currentFrame + 1 if the very next tick carries
  // it. A wrong frame here propagates to every late joiner, which replays the
  // input on the wrong tick and desyncs permanently.
  //
  // `frame === undefined` means "not broadcast yet" (see types.ts
  // NetworkInput.frame). Those inputs are not lost by omitting them: the caller
  // receives them on the authority's next BROADCAST_INPUTS, which is the channel
  // that also tells it which frame they belong to.
  const broadcastInputs = room.inputs.filter(input => (input as any).frame !== undefined);

  return {
    id: room.id,
    snapshot: room.snapshot,
    snapshotHash: room.snapshotHash,
    snapshotTimestamp: room.snapshotTimestamp,
    inputs: broadcastInputs,
    events: broadcastInputs, // Backwards compatibility
    frame: currentFrame,
    isAuthority: room.isAuthority
  };
});

// HTTP relay endpoint for reliable input delivery (used by replicas to send inputs to authority)
app.post<{ Params: { roomId: string }; Body: { input?: any; event?: any } }>(
  '/api/rooms/:roomId/relay',
  async (request, reply) => {
    const { roomId } = request.params;
    // Support both 'input' and 'event' for backwards compatibility
    const input = request.body?.input || request.body?.event;

    // Without this guard a body missing both keys throws on `input.seq` below
    // and surfaces as an opaque 500.
    if (!input || typeof input !== 'object') {
      return reply.code(400).send({ error: 'Body must contain an "input" object' });
    }

    // The sharpest thing on this surface. An input that arrives on a client
    // socket is stamped with the sender from that socket's own token, which is
    // what makes attribution work; an input that arrives here carries whatever
    // clientId the caller wrote, and is broadcast to every client and every peer
    // as though the room had agreed to it. Unauthenticated, that is "act as any
    // player in any room on this node" for anyone who can reach the port.
    if (!requireMeshAuth(request, reply, 'relay', `relay into room ${roomId}`, roomId)) return;

    const room = roomManager.getRoom(roomId);

    if (!room) {
      return reply.code(404).send({ error: 'Room not found' });
    }

    if (!room.isAuthority) {
      return reply.code(400).send({ error: 'Not authority for this room' });
    }

    // Skip inputs that already have sequence numbers
    if (input.seq !== undefined && input.seq > 0) {
      return { success: true, seq: input.seq, skipped: true };
    }

    // Add input and broadcast
    const seq = roomManager.addInput(roomId, input);
    if (seq) {
      // Queue the very object that went into room.inputs, not a copy.
      //
      // sendTick stamps input.frame on whatever it broadcasts. Queuing a copy
      // meant only the copy ever got a frame, while the entry sitting in
      // room.inputs kept frame === undefined forever - and `frame === undefined`
      // is exactly what every catch-up filter treats as "not broadcast yet, skip
      // it". So every input a replica relayed here was broadcast live but was
      // then invisible in the room's history: a client joining afterwards was
      // never told about it. For a join input that means a player nobody who
      // arrived later can see.
      input.seq = seq;
      queueInputForPeers(roomId, input, peerManager);
      queueInputForClients(roomId, input, peerManager);
      return { success: true, seq };
    }

    return { success: false, error: 'Failed to add input' };
  }
);

// Register node with centralized service
async function registerWithCentralService() {
  try {
    const response = await fetch(`${CENTRAL_SERVICE_URL}/api/nodes`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(ARRR_NODE_TOKEN ? { Authorization: `Bearer ${ARRR_NODE_TOKEN}` } : {})
      },
      body: JSON.stringify({
        id: NODE_ID,
        name: NODE_NAME,
        connectionUrl: NODE_PUBLIC_URL,
        // Where this node's own HTTP API lives, which is not always derivable
        // from the address clients dial. A deployment that tunnels the node
        // through the central service - so that only one port has to be
        // reachable from a browser - advertises central's address here, and
        // everything that turned that into an API base then called central
        // instead of the node. Replica-to-authority input relays went to a
        // service with no such route and returned 404, leaving every client on
        // the replica connected but never joining the simulation.
        apiUrl: NODE_API_PUBLIC_URL,
        location: 'local'
      })
    });

    if (!response.ok) {
      // Central says why it refused - a missing or revoked token, most often -
      // and that reason is the whole fix, so it goes in the error rather than
      // a bare status. Registration is retried regardless: a token created in
      // the portal a minute from now should be picked up without a restart.
      let reason = response.statusText;
      try {
        const body = await response.json() as { error?: string };
        if (body?.error) reason = body.error;
      } catch { /* not JSON; the status text will do */ }
      const hint = (response.status === 401 || response.status === 403) && !ARRR_NODE_TOKEN
        ? ' (no ARRR_NODE_TOKEN is set)'
        : '';
      throw new Error(`Registration refused (${response.status}): ${reason}${hint}`);
    }

    const data = await response.json() as { success: boolean; node: { id: string; validationKey: string } };
    registeredNodeId = data.node.id;
    nodeValidationKey = data.node.validationKey;

    // Update peer manager to use registered ID
    peerManager.setNodeId(registeredNodeId);
    console.log('Registered with central service:', { nodeId: registeredNodeId, hasValidationKey: !!nodeValidationKey });

    // Note: Peer connections are established on-demand via /api/cluster/connect
    // when the central service assigns rooms to this node
  } catch (error) {
    console.error('Failed to register with central service:', error);
    // Retry registration in 5 seconds
    setTimeout(registerWithCentralService, 5000);
  }
}

// Verification endpoint called by Central Service
app.get('/api/auth/verify', async (request, reply) => {
  // Return our current ephemeral ID
  // This proves we are the process currently listening at this URL
  return { id: NODE_ID };
});

/**
 * Tell central we are still here - and register again if it has forgotten us.
 *
 * Central answers 404 for a node it does not know, and this used to ignore the
 * response entirely. Central holds its node list in memory, so a restart of it
 * loses every registration; the nodes carry on heartbeating into a 404 forever
 * and central never learns they exist. Rooms already in progress are unaffected
 * - a client that is playing talks only to its node - but no new player can
 * join anything, because central has nowhere to send them, and nothing recovers
 * until every node is restarted by hand.
 *
 * Measured: with three clients playing, killing central left them running
 * perfectly, and a client arriving after central came back could not join at
 * all. Central was up and answering, and had no nodes to offer it.
 *
 * Registering again is the whole fix. It is idempotent - central verifies the
 * node is reachable and stores it - and it only happens when central says it
 * has never heard of us.
 */
async function sendHeartbeat() {
  if (!registeredNodeId) return;

  try {
    const res = await fetch(`${CENTRAL_SERVICE_URL}/api/nodes/${registeredNodeId}/heartbeat`, {
      method: 'POST'
    });
    if (res.status === 404) {
      console.warn(`[HEARTBEAT] Central does not know node ${registeredNodeId} - registering again`);
      await registerWithCentralService();
    }
  } catch (error) {
    console.error('Failed to send heartbeat:', error);
  }
}

async function start() {
  try {
    await app.listen({ port: NODE_PORT, host: '0.0.0.0' });
    console.log(`Node ${NODE_ID} started on port ${NODE_PORT}`);

    // Said once at startup as well as on every request it permits. A node left
    // in warn mode is indistinguishable from a secured one until something
    // actually tries, and by then the line is buried in traffic.
    if (meshAuthMode === 'warn') {
      console.warn(
        '[CONFIG] MESH_AUTH_MODE=warn: unauthenticated control-plane calls - input relays, ' +
          'authority claims, peer links, room deletes - will be PERMITTED and logged rather ' +
          'than refused. Intended only for a rolling upgrade; unset it once every node is up.'
      );
    }

    if (!ARRR_NODE_TOKEN) {
      console.log('[CONFIG] ARRR_NODE_TOKEN is not set: registering as an unowned node. Fine locally; a public cloud will refuse it.');
    }

    // Set the node's connection URL
    peerManager.setNodeUrl(NODE_PUBLIC_URL);

    // Register with centralized service
    await registerWithCentralService();

    // Send heartbeat every 10 seconds
    setInterval(sendHeartbeat, 10000);
  } catch (err) {
    app.log.error(err);
    process.exit(1);
  }
}

start();
