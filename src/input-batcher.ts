import { NetworkInput, MessageType } from './types';
import { roomManager } from './room-manager';
import type { PeerManager } from './peer-manager';
import { encodeTick } from './binary-protocol';

// Default tick rate (can be overridden per-room)
const DEFAULT_TICK_RATE_HZ = 20;

// Per-room tick state
interface RoomTickState {
  frame: number;
  pendingInputs: NetworkInput[];
  pendingPeerInputs: NetworkInput[];
  tickInterval: NodeJS.Timeout | null;
  peerManager: PeerManager | null;
  tickRateHz: number;
}

const roomTickStates = new Map<string, RoomTickState>();

function getOrCreateTickState(roomId: string, tickRateHz?: number): RoomTickState {
  if (!roomTickStates.has(roomId)) {
    roomTickStates.set(roomId, {
      frame: 0,
      pendingInputs: [],
      pendingPeerInputs: [],
      tickInterval: null,
      peerManager: null,
      tickRateHz: tickRateHz || DEFAULT_TICK_RATE_HZ
    });
  }
  return roomTickStates.get(roomId)!;
}

// Send a tick to all clients - ALWAYS send for frame sync
function sendTick(roomId: string) {
  const tickState = roomTickStates.get(roomId);
  if (!tickState) return;

  const room = roomManager.getRoom(roomId);
  if (!room) {
    // Room gone, stop ticking
    stopRoomTick(roomId);
    return;
  }

  // Check if room has any active connections
  const clients = roomManager.getClients(roomId);
  const peers = tickState.peerManager?.getPeers().filter(p => p.isConnected) || [];

  // No active connections - pause ticking to save resources
  if (clients.length === 0 && peers.length === 0) {
    console.log(`[TICK] Pausing tick for room ${roomId} - no active connections`);
    stopRoomTick(roomId);
    return;
  }

  // Increment frame
  tickState.frame++;

  // Collect pending inputs
  const inputs = tickState.pendingInputs;
  tickState.pendingInputs = [];

  // Inputs already have seq assigned in arrival order (see roomManager.addInput).
  // This IS the deterministic order - all clients receive inputs in this order.
  // No sorting needed - just stamp the broadcast frame.
  for (const input of inputs) {
    if (process.env.TICK_TRACE && input.frame !== undefined && input.frame !== tickState.frame) {
      console.log(`[RESTAMP-CLIENT] room=${roomId} seq=${input.seq} ${input.frame} -> ${tickState.frame}`);
    }
    input.frame = tickState.frame;
  }
  if (process.env.TICK_TRACE && inputs.length) {
    console.log(`[TICK-TRACE] room=${roomId} frame=${tickState.frame} seqs=${inputs.map((i: any) => i.seq).join(',')}`);
  }

  // ALWAYS send tick to clients for frame synchronization
  // Even empty ticks are needed so clients can sync their local frame counter
    const joinInputs = inputs.filter((i: any) => i.type === 'join' || i.data?.type === 'join');
    if (joinInputs.length > 0) {
      console.log(`[TICK] frame=${tickState.frame} room=${roomId} sending ${joinInputs.length} join inputs to ${clients.length} clients: ${clients.map(c => c.id.slice(0, 8)).join(',')}`);     }
  if (clients.length > 0) {
    // Get snapshot frame for late joiner catchup logic
    const snapshotFrame = room.snapshot?.frame || 0;

    // Get majority hash from partition collector (consensus-based state sync)
    // This is the hash that most clients agree on - clients compare against this
    // Note: snapshotHash removed - majorityHash provides consensus, no need for redundant SHA-256
    const majorityHash = roomManager.getMajorityHash(roomId, tickState.frame - 1) ?? undefined;
    // Debug: log every 100 frames
    if (tickState.frame % 100 === 0) {
      console.log(`[TICK] frame=${tickState.frame} majorityHash(${tickState.frame - 1})=${majorityHash?.toString(16) ?? 'none'}`);
    }

    const message = encodeTick(tickState.frame, inputs, undefined, snapshotFrame, majorityHash);

    // Only log large ticks (potential issue)
    if (message.length > 500) {
      console.log(`[TICK-SIZE] frame=${tickState.frame} msgSize=${message.length} inputs=${inputs.length}`);
    }

    clients.forEach(client => {
      // Skip clients who haven't received INITIAL_STATE yet
      if (!client.initialStateReceived) return;
      try {
        client.socket.send(message);
      } catch (err) {
        console.error(`Failed to send tick to client ${client.id}:`, err);
      }
    });
  }

  // Drained every tick, whether or not there is anybody to send it to.
  //
  // This clearing used to live inside the check below, so a node with no peers
  // pushed every input from every client into this array and never emptied it.
  // A single node is the default deployment and what the demos run, so the
  // queue grew for the life of the process: measured at about 45,000 retained
  // input objects in eight minutes, roughly 100MB an hour, on a room that was
  // otherwise completely steady.
  //
  // It hid well. Every counter the node reports stayed flat throughout, because
  // this array is in the tick batcher rather than on the room, and nothing was
  // counting it. What found it was a heap snapshot diff: 45,432 new objects
  // held as array elements, each with an `id` and a `data` field, which is the
  // shape of a NetworkInput and nothing else.
  const peerInputs = tickState.pendingPeerInputs;
  tickState.pendingPeerInputs = [];

  // ALWAYS send to peers for frame sync (including empty ticks)
  if (tickState.peerManager && peers.length > 0) {

    // Order peer inputs by seq - the same order this node just served its own
    // clients.
    //
    // This used to sort by (frame, clientId). Both queues hold the *same input
    // objects*, and the loop above has already stamped every one of them with
    // this tick's frame, so the frame comparison was always 0 and it degenerated
    // to sorting by clientId. Authority-local clients got arrival order while
    // replica clients got clientId order, which is a different order for the
    // same inputs - a guaranteed lockstep desync between two clients in the same
    // room on different nodes, for any input that is not commutative.
    //
    // seq is assigned in arrival order by roomManager.addInput and is a total
    // order, so this also re-sorts correctly if a peer link was down and the
    // queue built up a backlog across several ticks.
    peerInputs.sort((a, b) => (a.seq || 0) - (b.seq || 0));

    // CRITICAL: Stamp ALL peer inputs with the tick frame they were broadcast in.
    for (const input of peerInputs) {
      if (process.env.TICK_TRACE && input.frame !== undefined && input.frame !== tickState.frame) {
        console.log(`[RESTAMP-PEER] room=${roomId} seq=${input.seq} ${input.frame} -> ${tickState.frame}`);
      }
      input.frame = tickState.frame;
    }

    if (peerInputs.length > 0) {
      const seqs = peerInputs.map((e: any) => e.seq).join(',');
      console.log(`[TICK] frame=${tickState.frame}, room=${roomId}, peerInputSeqs=[${seqs}], peers=${peers.length}`);
    }

    tickState.peerManager.broadcastToPeers({
      type: MessageType.BROADCAST_INPUTS,
      payload: {
        roomId,
        frame: tickState.frame,
        inputs: peerInputs
      }
    });
  }
}

/**
 * How many inputs are queued for peers across all rooms.
 *
 * Reported so this array is visible from outside the process. It grew
 * unboundedly for the life of a single-node deployment and every health counter
 * said the node was fine, because none of them could see it.
 */
/**
 * How many rooms this node still holds tick state for.
 *
 * Compared against the number of rooms that actually exist, this says whether
 * state is outliving its room. The frame counter is deliberately kept when a
 * room stops ticking, so a rejoining client resumes at the right frame - but
 * that is only meant to outlive the tick, not the room.
 */
export function tickStateCount(): number {
  return roomTickStates.size;
}

export function pendingPeerInputCount(): number {
  let total = 0;
  for (const state of roomTickStates.values()) total += state.pendingPeerInputs?.length ?? 0;
  return total;
}

// Start ticking for a room
export function startRoomTick(roomId: string, peerManager: PeerManager, tickRateHz?: number) {
  const tickState = getOrCreateTickState(roomId, tickRateHz);
  tickState.peerManager = peerManager;
  if (tickRateHz) tickState.tickRateHz = tickRateHz;

  if (!tickState.tickInterval) {
    const intervalMs = 1000 / tickState.tickRateHz;
    console.log(`[TICK] Starting ${tickState.tickRateHz}Hz tick for room ${roomId}`);
    // Nothing a single room does may take the process down. sendTick runs on a
    // bare timer, so anything it throws is an unhandled exception in a timer
    // callback and Node ends the process - every other room, every player,
    // gone. That is not hypothetical: an input too large for the tick encoding
    // threw here and killed the node. The encoder no longer produces that, but
    // the timer should never have been the thing standing between one room's
    // bad tick and the whole service.
    tickState.tickInterval = setInterval(() => {
      try {
        sendTick(roomId);
      } catch (err) {
        console.error(`[TICK] Room ${roomId} threw while ticking, and was not allowed to take the node with it:`, err);
      }
    }, intervalMs);
  }
}

// Stop ticking for a room (but preserve frame counter!)
export function stopRoomTick(roomId: string) {
  const tickState = roomTickStates.get(roomId);
  if (tickState?.tickInterval) {
    console.log(`[TICK] Stopping tick for room ${roomId} at frame ${tickState.frame}`);
    clearInterval(tickState.tickInterval);
    tickState.tickInterval = null;
  }
  // DON'T delete the tick state - we need to preserve the frame counter
  // for when new clients join. Only the interval is stopped.
}

/**
 * Drop tick state for rooms this node no longer has.
 *
 * cleanupRoomTick handles the ordinary path: a room is deleted here and its
 * tick state goes with it. It does not cover state created for a room this node
 * never hosted, because no deletion ever happens for one - and getOrCreateTickState
 * will make an entry for any id it is handed, including a room id that only
 * exists on the other node.
 *
 * Measured on a two-node session: 19 and 14 tick states against 3 live rooms,
 * climbing by one for every room the other node had ever hosted. The immediate
 * cause is fixed at the call site, and this is here so the next one is bounded
 * rather than permanent - it is a map that only ever grew.
 *
 * Returns how many were dropped, so the sweep can say when it did something.
 */
const ORPHAN_TICK_STATE_TTL = 60_000;

export function dropOrphanedTickStates(hasRoom: (roomId: string) => boolean): number {
  const now = Date.now();
  let dropped = 0;
  for (const roomId of [...roomTickStates.keys()]) {
    if (hasRoom(roomId)) continue;
    const tickState = roomTickStates.get(roomId);
    // A room that is mid-arrival has a live interval or queued work; leave it.
    if (tickState?.tickInterval) continue;
    if (tickState?.pendingInputs.length || tickState?.pendingPeerInputs.length) continue;
    // And leave anything still being written to.
    //
    // This is why the check is not simply "no room here". The frame is tracked
    // for a room that has not arrived yet, deliberately, so that a client
    // joining the moment it does can be told what frame it is on. Sweeping on
    // absence alone deleted that frame mid-arrival and the joining client was
    // told frame 0 - which is the same failure as moving the setCurrentFrame
    // call, arrived at from the other direction, and it took a full suite run
    // to show because it needs the sweep to land inside a window of a few
    // milliseconds.
    //
    // A minute of silence is a wide margin over a window that size, and still
    // bounds the map.
    const touchedAt = (tickState as any)?.touchedAt || 0;
    if (now - touchedAt < ORPHAN_TICK_STATE_TTL) continue;
    roomTickStates.delete(roomId);
    dropped++;
  }
  return dropped;
}

// Fully clean up a room's tick state (call when room is deleted)
export function cleanupRoomTick(roomId: string) {
  stopRoomTick(roomId);
  roomTickStates.delete(roomId);
  console.log(`[TICK] Cleaned up tick state for deleted room ${roomId}`);
}

// Get current frame for a room (for INITIAL_STATE)
export function getCurrentFrame(roomId: string): number {
  const tickState = roomTickStates.get(roomId);
  return tickState?.frame || 0;
}

// Set current frame for a room (used by replicas when receiving BROADCAST_INPUTS)
export function setCurrentFrame(roomId: string, frame: number): void {
  const tickState = getOrCreateTickState(roomId);
  tickState.frame = frame;
  (tickState as any).touchedAt = Date.now();
}

// Queue input to be sent on next tick
// IMPORTANT: Only authority nodes should run their own tick interval.
// Replica nodes receive TICKs from authority via BROADCAST_INPUTS and forward them.
export function queueInputForClients(roomId: string, input: NetworkInput, peerManager: PeerManager) {
  const room = roomManager.getRoom(roomId);

  // Only queue and tick on authority nodes
  // Replica nodes forward TICK from authority via peer-handler
  if (!room?.isAuthority) {
    return;
  }

  const tickState = getOrCreateTickState(roomId);
  tickState.pendingInputs.push(input);

  // Ensure tick is running
  if (!tickState.tickInterval) {
    startRoomTick(roomId, peerManager);
  }
}

export function queueInputForPeers(roomId: string, input: NetworkInput, peerManager: PeerManager) {
  const room = roomManager.getRoom(roomId);

  // Only authority nodes broadcast to peers via their tick
  // Replica nodes send inputs directly to authority via RELAY_INPUT
  if (!room?.isAuthority) {
    return;
  }

  const tickState = getOrCreateTickState(roomId);
  tickState.pendingPeerInputs.push(input);

  // Ensure tick is running
  if (!tickState.tickInterval) {
    startRoomTick(roomId, peerManager);
  }
}

// Backwards compatibility aliases (will be removed)
export function queueEventForClients(roomId: string, event: NetworkInput, peerManager: PeerManager) {
  queueInputForClients(roomId, event, peerManager);
}
export function queueEventForPeers(roomId: string, event: NetworkInput, peerManager: PeerManager) {
  queueInputForPeers(roomId, event, peerManager);
}

// Legacy function name for compatibility
export function flushBatch(roomId: string, peerManager: PeerManager) {
  // Now handled by tick system
}
