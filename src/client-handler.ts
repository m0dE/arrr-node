import * as jwt from 'jsonwebtoken';
import { Message, MessageType, NetworkInput } from './types';
import { roomManager } from './room-manager';
import { PeerManager } from './peer-manager';
import { handlePeerConnection } from './peer-handler';
import { syncMasterClientList } from './sync-utils';
import { queueInputForClients, queueInputForPeers, getCurrentFrame, setCurrentFrame, startRoomTick, stopRoomTick } from './input-batcher';
import {
  encodeRoomCreated,
  encodeInitialState,
  encodeRoomJoined,
  encodeRoomLeft,
  encodeError,
  encodeSnapshotUpdate,
  encodeClientListUpdate,
  BinaryMessageType,
  decodeMessage
} from './binary-protocol';
import { getNodeValidationKey } from './index';
import { voiceRelay, VOICE_READY_PACKET } from './voice-relay';
import { selfAuthHeaders } from './mesh-auth';
import type WebSocket from 'ws';
import { wsToHttp, generateInputId, peerUrlFromApi, badRoomId } from './utils';

const BINARY_INPUT_MARKER = 0x20;
const BINARY_SNAPSHOT_MARKER = 0x21;
const BINARY_SNAPSHOT_V2_MARKER = 0x22;  // With seq/frame header
const BINARY_SNAPSHOT_V3_MARKER = 0x23;  // With seq/frame/hash header

interface JwtPayload {
  clientId: string;
  /** Present only when central verified a signed-in account for this connection. */
  authenticatedUserId?: string;
  clientMetadata?: Record<string, any>;
  roomId: string;
  /** The application central authorised this connection for; absent from tokens minted by the legacy per-room route. */
  appId?: string;
  isMuted?: boolean;
  readOnly?: boolean;
  exp?: number;
}

/**
 * Name a client whose simulation code differs from the room's.
 *
 * Lockstep cannot make a client agree if it is running different rules, so this
 * is recorded through the same channel as a desync but labelled with the reason.
 * Without it the only symptom is an endless stream of hash mismatches that looks
 * exactly like a transport fault.
 */
/**
 * Per-client input allowance: a sustained rate with room for a burst.
 *
 * Nothing limited this. In lockstep every input is broadcast to every client,
 * so a flooder multiplies its own traffic by the room size: one measured
 * against three well-behaved clients pushed their downstream from 1.3KB/s to
 * 188KB/s, a 145x amplification.
 *
 * The first attempt was a flat cap per tick, which is the wrong shape and the
 * suite caught it immediately - test-tick-input-count deliberately sends a
 * 400-input burst to prove the 16-bit count field carries it, and a cap of ten
 * dropped 390. A one-off burst is harmless; what costs the room is a client
 * sending thousands a second forever. So: refill steadily, allow a large burst
 * to be spent at once, and throttle only what is sustained beyond it.
 */
/**
 * Largest input a client may send.
 *
 * The rate limit below caps how MANY inputs arrive, never how large they are,
 * and size is the same weapon in a different hand: every input is broadcast to
 * every client in the room and held in the node's backlog until it is pruned,
 * so one client can make a room pay for whatever it feels like sending. The
 * snapshot path was bounded for exactly this reason and its comment names the
 * input path as the same shape - which it was, and which nothing checked.
 *
 * Worse than fan-out, it was fatal. The tick format writes each input's length
 * as a UInt16, so anything over 65535 bytes throws while being encoded, inside
 * the room's tick timer, where nothing catches it. A single 400KB message
 * killed the node process and every room on it. Found by sending one.
 *
 * Eight kilobytes is enormous for an input - the demos send tens of bytes, and
 * a chat line is stored truncated to eighty characters - while leaving room for
 * an application with genuinely chunky inputs.
 */
const MAX_INPUT_BYTES = Number(process.env.MAX_INPUT_BYTES || 8192);
/**
 * Longest room id a client may ask for.
 *
 * Ids arrive from whoever is connecting and were never checked. A room id is
 * not a one-off request - it is stored, logged on every join, sent to peers,
 * and written into the wire format with a UInt16 length field, so an id of tens
 * of kilobytes is a cost the room carries in every message about it, forever.
 *
 * This path is luckier than the input one that killed the node: it runs inside
 * the socket's message handler, which catches, so an unencodable id costs that
 * client rather than the process. Luckier is not bounded. Two hundred and fifty
 * six characters is far beyond anything real - the demos use names like
 * "lobby-fps3d" and the suite's are a prefix and a timestamp.
 */
const INPUT_RATE_PER_SEC = Number(process.env.INPUT_RATE_PER_SEC || 60);
const INPUT_BURST = Number(process.env.INPUT_BURST || 600);

const oversizeWarnedAt = new Map<string, number>();


/**
 * How many bytes a JSON input will occupy once replicated.
 *
 * Measured rather than guessed from the object's shape: the thing that has to
 * fit on the wire is the serialised form, and a string of one character per
 * byte is not the only way to be large.
 */
function jsonInputBytes(data: unknown): number {
  if (data === undefined || data === null) return 0;
  try {
    return Buffer.byteLength(JSON.stringify(data), 'utf8');
  } catch {
    // Circular or otherwise unserialisable: it cannot be replicated, so treat
    // it as oversized rather than letting it through unmeasured.
    return Number.MAX_SAFE_INTEGER;
  }
}

/** True when this input is too big to accept, complaining at most once a second. */
function oversizedInput(clientId: string, size: number, kind: string): boolean {
  if (size <= MAX_INPUT_BYTES) return false;
  const now = Date.now();
  const last = oversizeWarnedAt.get(clientId) || 0;
  if (now - last > 1000) {
    oversizeWarnedAt.set(clientId, now);
    console.error(`[INPUT] Rejecting ${size}-byte ${kind} from ${clientId}: the ceiling is ${MAX_INPUT_BYTES} bytes`);
  }
  return true;
}
const inputBudget = new Map<string, { tokens: number; last: number; warnedAt: number }>();

/**
 * True when this input should be dropped because the client is over its
 * sustained allowance.
 *
 * Dropping is safe for everyone else: the input is never sequenced, so it
 * reaches nobody and no client's world depends on it. The flooder loses its own
 * excess, which is the intended outcome.
 */
function overInputBudget(clientId: string): boolean {
  const now = Date.now();
  let b = inputBudget.get(clientId);
  if (!b) {
    b = { tokens: INPUT_BURST, last: now, warnedAt: 0 };
    inputBudget.set(clientId, b);
  }
  b.tokens = Math.min(INPUT_BURST, b.tokens + ((now - b.last) / 1000) * INPUT_RATE_PER_SEC);
  b.last = now;

  if (b.tokens >= 1) { b.tokens -= 1; return false; }

  if (now - b.warnedAt > 5000) {
    b.warnedAt = now;
    console.warn(
      `[FLOOD] client ${clientId} is over its sustained input allowance ` +
      `(${INPUT_RATE_PER_SEC}/s, burst ${INPUT_BURST}); dropping the excess (this client only)`
    );
  }
  return true;
}

/** Forget a client's allowance when it goes away. */
function clearInputBudget(clientId: string): void {
  inputBudget.delete(clientId);
}

function checkClientSimVersion(roomId: string, clientId: string, metadata: any): void {
  const mismatch = roomManager.checkSimVersion(roomId, clientId, metadata);
  if (!mismatch) return;
  console.warn(
    `[SIM-VERSION] ${mismatch.playerId} joined ${roomId} running simulation ${mismatch.theirs}, ` +
    `but the room is running ${mismatch.expected}. This client cannot agree with the others ` +
    `until it reloads - a stale build, not a network fault.`
  );
  roomManager.recordDesyncReport(roomId, {
    clientId,
    receivedAt: Date.now(),
    frame: 0,
    mine: 0,
    majority: 0,
    player: mismatch.playerId,
    reason: 'version-mismatch',
    simVersion: mismatch.theirs,
    expectedSimVersion: mismatch.expected,
  });
}

/**
 * Is this connection allowed into this room?
 *
 * Central mints a connect token for one room and signs it with the node's
 * validation key. The node verified the signature and then never looked at the
 * room: JOIN_ROOM took whichever room the client asked for, and the token's
 * roomId appeared only in a log line. So a token issued for any room admitted
 * its holder to every room, for the twenty-four hours it stayed valid.
 *
 * Measured before this: a token minted for "scope-mine" was used to join
 * "lobby-chat" - a different, live room - and the node answered ROOM_JOINED.
 *
 * That matters most for the applications this engine is meant to carry beyond
 * games. Deciding who may enter a room is central's job and the room is the
 * privacy boundary for anything chat-shaped; leaving the decision unenforced
 * makes the token a pass to everything rather than to one place.
 *
 * Two shapes are accepted because central mints two: the bare room id, and the
 * app-scoped `appId_roomId` that the per-app endpoint issues. Splitting at the
 * first underscore recovers the room from the second. A token carrying no room
 * at all is left alone rather than refused - that is a different decision from
 * this one, and refusing it here would break anyone minting tokens by hand.
 */
function tokenAllowsRoom(tokenRoomId: string | undefined, requested: string): boolean {
  if (!tokenRoomId) return true;
  if (tokenRoomId === requested) return true;
  const cut = tokenRoomId.indexOf('_');
  return cut > 0 && tokenRoomId.slice(cut + 1) === requested;
}

export function handleClientConnection(
  socket: WebSocket,
  peerManager: PeerManager,
  nodeId: string,
  jwtPayload: JwtPayload
) {
  const clientId = jwtPayload.clientId;
  const tokenRoomId = jwtPayload.roomId;
  /**
   * Who this connection is, when that was actually established.
   *
   * Room membership is keyed by `user.id` out of the client's own JOIN_ROOM
   * payload. With nobody signed in that is the only identity there is, and it
   * is fine - it is a display name, not a claim about who you are. With
   * somebody signed in it is a hole: a connection can name any member, and if
   * that member is inside their disconnect grace period the claim is taken as
   * a reconnect and this connection takes over their slot, appearing as them
   * in every client's roster.
   *
   * Central now says when it has verified an account, so the verified identity
   * wins where there is one. Where there is not, nothing changes.
   */
  const authenticatedUserId = jwtPayload.authenticatedUserId;
  const clientMetadata = jwtPayload.clientMetadata || {};
  const isMuted = jwtPayload.isMuted || false;
  const readOnly = jwtPayload.readOnly || false;
  let currentRoomId: string | undefined;
  /**
   * The member this connection joined as.
   *
   * Membership is keyed on `authenticatedUserId || user.id` when joining, but
   * every path that ends a connection - LEAVE_ROOM, and the socket closing -
   * used to re-derive it as `clientInfo.metadata.id`, which is the client's own
   * claim. For an anonymous connection the two are the same string and nothing
   * was wrong. For a signed-in one they are not, so the departure named a
   * member that does not exist: the leave removed nobody and the disconnect
   * marked nobody, which left the member permanently `connected` and therefore
   * permanently beyond the ghost sweep. A player who quit stayed in the world,
   * and on every leaderboard, for the life of the room.
   *
   * Recorded once, where it is decided, and read by everything that ends the
   * connection.
   */
  let memberOdId: string | undefined;

  console.log(`Client ${clientId} connected (app: ${jwtPayload.appId ?? 'none'}, muted: ${isMuted}, readOnly: ${readOnly})`);

  // Helper to check if client is currently muted in the room.
  // Mute state can change at runtime (via the master client list), so this reads
  // the live room entry and only falls back to the JWT claim.
  const getIsMuted = () => {
    if (!currentRoomId) return isMuted; // Fallback to initial state if no room joined yet
    const room = roomManager.getRoom(currentRoomId);
    if (!room) return isMuted;
    const client = room.clients.get(clientId);
    return client ? client.isMuted : isMuted;
  };

  // A client may not mutate room state if it is muted, or if its token was
  // issued read-only. `readOnly` is fixed for the life of the token.
  const isWriteBlocked = (what: string) => {
    if (readOnly) {
      console.log(`[SECURITY] Ignoring ${what} from read-only client ${clientId}`);
      return true;
    }
    if (getIsMuted()) {
      console.log(`[SECURITY] Ignoring ${what} from muted client ${clientId}`);
      return true;
    }
    return false;
  };

  socket.on('message', async (data: Buffer) => {
    try {
      // Voice first: it is the most frequent message a talking client sends,
      // and it never touches the room's state, sequence or hashes. Read-only
      // connections are observers and do not get a voice; muted ones are
      // told so by the relay, which is the one place the word is literal.
      if (data.length > 0 && data[0] === BinaryMessageType.VOICE) {
        if (!currentRoomId || readOnly) return;
        voiceRelay.onPacket(currentRoomId, clientId, memberOdId || clientId, socket, data, getIsMuted(), peerManager);
        return;
      }

      // Check if this is a binary SYNC_HASH message
      if (data.length > 0 && data[0] === BinaryMessageType.SYNC_HASH) {
        const decoded = decodeMessage(data);
        if (decoded && decoded.type === 'SYNC_HASH') {
          // Update client hash in room manager
          if (currentRoomId) {
            // Muted clients can still sync hashes for observability,
            // but they shouldn't be able to affect state.
            roomManager.updateClientHash(currentRoomId, clientId, decoded.hash, decoded.seq, decoded.frame);
          }
          return;
        }
      }

      // Check if this is a distributed state sync STATE_HASH message (0x30)
      if (data.length >= 9 && data[0] === BinaryMessageType.STATE_HASH) {
        const decoded = decodeMessage(data);
        if (decoded && decoded.type === 'STATE_HASH' && currentRoomId) {
          // A vote is a say in what the room believes, so a connection that may
          // not write does not get one.
          //
          // isWriteBlocked guards the four paths that change the world - inputs
          // and snapshots, in both encodings - and these two were missed. That
          // matters because of where read-only tokens come from: an application
          // with allowNonAuthRead hands one to anybody who asks with no
          // credentials at all, so the votes deciding whether honest players are
          // in step could be cast by unauthenticated observers, in any number.
          // Enough of them outvote the room.
          //
          // Read-only only, deliberately - not isWriteBlocked. Muting is a
          // moderation decision about speech: a muted client is still a real
          // participant running the real simulation, and its vote is as good as
          // anyone's. Read-only is a statement that this connection is not
          // trusted to affect the room, and consensus is the room.
          if (readOnly) {
            console.log(`[SECURITY] Ignoring state hash from read-only client ${clientId}`);
            return;
          }
          roomManager.addStateHash(currentRoomId, clientId, decoded.frame, decoded.stateHash);
          // Pass it on, so every node measures agreement across the whole room.
          //
          // A node used to compare its clients only against the others that
          // happened to share it. With a room split across nodes that is a
          // vote among whoever is nearby, and a client alone on a node is
          // compared against itself - measured, a client deliberately corrupted
          // while alone on its node ran on for six hundred frames with a
          // different world and never once registered as disagreeing.
          peerManager.broadcastToPeers({
            type: MessageType.RELAY_STATE_HASH,
            payload: { roomId: currentRoomId, clientId, frame: decoded.frame, stateHash: decoded.stateHash },
          });
          // Debug: log every 100 frames
          if (decoded.frame % 100 === 0) {
            console.log(`[STATE_HASH] client=${clientId.slice(0,8)} frame=${decoded.frame} hash=${decoded.stateHash.toString(16)}`);
          }
        }
        return;
      }

      // Check if this is a distributed state sync PARTITION_DATA message (0x31)
      if (data.length >= 8 && data[0] === BinaryMessageType.PARTITION_DATA) {
        const decoded = decodeMessage(data);
        if (decoded && decoded.type === 'PARTITION_DATA' && currentRoomId) {
          // Same reasoning, same collector. Nothing reads partition data today,
          // which makes accepting it from an untrusted connection all cost and
          // no benefit.
          if (readOnly) {
            console.log(`[SECURITY] Ignoring partition data from read-only client ${clientId}`);
            return;
          }
          roomManager.addPartitionData(currentRoomId, clientId, decoded.frame, decoded.partitionId, decoded.data);
        }
        return;
      }

      // Check if this is a binary input message - store raw bytes (server is agnostic)
      // Binary format: [marker:1][frame:4][data:...] - minimum 5 bytes header
      if (data.length >= 5 && data[0] === BINARY_INPUT_MARKER) {
        if (isWriteBlocked('binary input')) return;
        if (!currentRoomId) return;
        // Same budget as the JSON path; a flood over either costs the room the
        // same amount.
        if (overInputBudget(clientId)) return;
        if (oversizedInput(clientId, data.length, 'binary input')) return;

        const room = roomManager.getRoom(currentRoomId);
        if (!room) return;

        // Binary format from client: [marker:1][frame:4][data:...]
        // Extract client's frame from the binary header
        const view = new DataView(data.buffer, data.byteOffset);
        const clientFrame = view.getUint32(1, true);  // 4 bytes after marker
        const rawBytes = Buffer.from(data.slice(5));  // Skip marker (1) + frame (4)

        const input: NetworkInput = {
          id: generateInputId(),
          clientId,
          data: rawBytes,  // Raw opaque bytes (without frame header)
          type: 'binary',
          seq: 0,
          // CRITICAL: Don't set frame yet - it will be stamped in sendTick.
          // Setting frame here causes race condition where INITIAL_STATE includes
          // inputs with wrong frame (client's frame instead of broadcast tick frame).
          // Store client's frame separately for server-side sorting.
          clientFrame: clientFrame  // For sorting only, not the broadcast frame
        };

        if (room.isAuthority) {
          const seq = roomManager.addInput(currentRoomId, input);
          if (seq) {
            input.seq = seq;
            queueInputForPeers(currentRoomId, input, peerManager);
            queueInputForClients(currentRoomId, input, peerManager);
          }
        } else {
          peerManager.broadcastToPeers({
            type: MessageType.RELAY_INPUT,
            payload: { roomId: currentRoomId, input }
          });
        }
        return;
      }

      // Check if this is a binary snapshot message (v1, v2, or v3 format)
      const isBinarySnapshotV1 = data.length > 0 && data[0] === BINARY_SNAPSHOT_MARKER;
      const isBinarySnapshotV2 = data.length > 0 && data[0] === BINARY_SNAPSHOT_V2_MARKER;
      const isBinarySnapshotV3 = data.length > 0 && data[0] === BINARY_SNAPSHOT_V3_MARKER;
      if (isBinarySnapshotV1 || isBinarySnapshotV2 || isBinarySnapshotV3) {
        if (isWriteBlocked('binary snapshot')) return;
        if (!currentRoomId) return;

        const room = roomManager.getRoom(currentRoomId);
        if (!room) return;

        if (isBinarySnapshotV3) {
          // Binary format v3: [marker:1][seq:4][frame:4][hashLen:1][hash:hashLen][binary:...]
          const view = new DataView(data.buffer, data.byteOffset);
          const seq = view.getUint32(1, true);
          const frame = view.getUint32(5, true);
          const hashLen = data[9];
          const hash = hashLen > 0 ? data.slice(10, 10 + hashLen).toString('utf8') : '';
          const rawBytes = Buffer.from(data.slice(10 + hashLen));

          // Update decoded snapshot with seq/frame/hash for input filtering and drift detection
          roomManager.updateSnapshot(currentRoomId, { seq, frame }, hash);
          console.log(`[SNAPSHOT] Binary snapshot v3 from ${clientId}: seq=${seq}, frame=${frame}, hash=${hash.slice(0,8)}`);

          // Update snapshot as opaque binary (stored for late joiners only)
          roomManager.updateBinarySnapshot(currentRoomId, rawBytes);
          console.log(`[SNAPSHOT] Binary snapshot v3 stored from ${clientId}, ${rawBytes.length} bytes (no broadcast - state sync handles consistency)`);
          // With distributed state sync, we don't broadcast snapshots to other clients.
          // Snapshots are only stored for late joiners.
          return;
        }

        if (isBinarySnapshotV2) {
          // Binary format v2 (legacy): [marker:1][seq:4][frame:4][binary:...]
          const view = new DataView(data.buffer, data.byteOffset);
          const seq = view.getUint32(1, true);
          const frame = view.getUint32(5, true);
          const rawBytes = Buffer.from(data.slice(9));

          // Update decoded snapshot with seq/frame for input filtering (no hash in v2)
          roomManager.updateSnapshot(currentRoomId, { seq, frame }, '');
          console.log(`[SNAPSHOT] Binary snapshot v2 (legacy) from ${clientId}: seq=${seq}, frame=${frame}`);

          // Update snapshot as opaque binary (stored for late joiners only)
          roomManager.updateBinarySnapshot(currentRoomId, rawBytes);
          console.log(`[SNAPSHOT] Binary snapshot v2 stored, ${rawBytes.length} bytes (no broadcast)`);
          // No broadcast - state sync handles consistency
          return;
        }

        // Legacy v1 format: [marker:1][binary:...]
        const rawBytes = Buffer.from(data.slice(1));

        // Update snapshot as opaque binary (stored for late joiners only)
        roomManager.updateBinarySnapshot(currentRoomId, rawBytes);
        console.log(`[SNAPSHOT] Binary snapshot v1 stored from ${clientId}, ${rawBytes.length} bytes (no broadcast)`);
        // No broadcast - state sync handles consistency

        return;
      }

      const message: Message = JSON.parse(data.toString());

      switch (message.type) {
        case MessageType.CREATE_ROOM: {
          const { roomId, snapshot, user } = message.payload;
          if (!tokenAllowsRoom(tokenRoomId, roomId)) {
            console.error(`[SECURITY] ${clientId} presented a token for ${tokenRoomId} `
              + `and asked to create ${roomId}; refusing`);
            socket.send(JSON.stringify({
              type: MessageType.ERROR,
              payload: { message: 'this connection is not authorised for that room' },
            }));
            break;
          }
          const badCreate = badRoomId(roomId);
          if (badCreate) {
            console.error(`[CREATE_ROOM] Refusing ${clientId}: ${badCreate}`);
            socket.send(JSON.stringify({ type: MessageType.ERROR, payload: { message: badCreate } }));
            break;
          }
          const centralServiceUrl = process.env.CENTRAL_SERVICE_URL || 'http://localhost:9001';

          // Check if room already exists (race condition with multiple clients)
          let room = roomManager.getRoom(roomId);
          if (room) {
            // Room already exists - treat this as a JOIN instead
            console.log(`[CREATE_ROOM] Room ${roomId} already exists, treating as JOIN`);

            // Merge user from message with JWT metadata for disconnect events
            const mergedMetadata = { ...user, ...clientMetadata };
            roomManager.addClient(roomId, clientId, socket, nodeId, isMuted, mergedMetadata);
            checkClientSimVersion(roomId, clientId, mergedMetadata);
            currentRoomId = roomId;

            // Check if this is a reconnecting member
            const odId = authenticatedUserId || user?.id;
            memberOdId = odId;
            const existingMember = odId ? roomManager.getMemberByOdId(roomId, odId) : undefined;
            const isReconnect = existingMember && existingMember.status === 'disconnected';

            // SERVER-AUTHORITATIVE: Generate join or reconnect input FIRST
            // so it can be included in INITIAL_STATE
            const inputType = isReconnect ? 'reconnect' : 'join';
            const connectionInput: NetworkInput = {
              id: generateInputId(),
              clientId,
              type: inputType,
              data: { type: inputType, clientId, user },
              seq: 0
              // frame is deliberately NOT set here. sendTick() stamps the frame the
              // input is actually broadcast in, which is the NEXT tick - stamping the
              // current frame on arrival makes room.inputs claim a frame no client has
              // received yet, and a late joiner rebuilding from that history replays
              // the input a tick early and desyncs permanently. `frame === undefined`
              // is also how every catch-up filter recognises "not broadcast yet".
              // See types.ts NetworkInput.frame.
            };

            // Update membership
            if (odId && existingMember) {
              // Connected either way: a member that is still marked connected
              // is one whose previous socket has not finished closing, and the
              // member has to name this connection before that close arrives -
              // otherwise the stale close demotes a member who is live. See the
              // ownership guard in the socket close handler.
              roomManager.setMemberStatus(roomId, odId, 'connected', clientId);
              console.log(
                `[${isReconnect ? 'RECONNECT' : 'REJOIN'}] Member ${odId} is now client ` +
                `${clientId} (via CREATE_ROOM)`
              );
            } else if (odId) {
              roomManager.addMember(roomId, odId, clientId, user);
            }

            // Add join event to room BEFORE filtering inputs for INITIAL_STATE
            if (room.isAuthority) {
              const seq = roomManager.addInput(roomId, connectionInput);
              if (seq) {
                connectionInput.seq = seq;
                queueInputForPeers(roomId, connectionInput, peerManager);
                queueInputForClients(roomId, connectionInput, peerManager);
              }
              // Authority: Sync master list to everyone
              syncMasterClientList(roomId, room, peerManager);
            } else {
              peerManager.broadcastToPeers({
                type: MessageType.RELAY_INPUT,
                payload: { roomId, input: connectionInput }
              });
            }

            // If this is a replica node, notify authority of new client
            if (!room.isAuthority) {
              peerManager.broadcastToPeers({
                type: MessageType.RELAY_CLIENT_JOIN,
                payload: { roomId, clientId, nodeId, isMuted, metadata: mergedMetadata }
              });
              console.log(`[MASTER_CLIENT_LIST] Sent RELAY_CLIENT_JOIN for ${clientId} to peers (authority will handle)`);
            }

            // Same rule as every other catch-up path - see selectCatchUpInputs.
            const inputsSnapshot = roomManager.selectCatchUpInputs(roomId, clientId);
            const currentFrame = getCurrentFrame(roomId);

            // Send initial state with join event included
            console.log(`[CREATE_ROOM->JOIN] Sending inputs=${inputsSnapshot.length} (filtered from ${room.inputs.length})`);
            console.log(`[CREATE_ROOM->JOIN] Snapshot: frame=${room.snapshot?.frame}, hash=${room.snapshotHash}`);
            socket.send(encodeRoomJoined(roomId, clientId));
          socket.send(VOICE_READY_PACKET);
            socket.send(encodeInitialState(roomId, currentFrame, room.snapshot, room.snapshotHash, inputsSnapshot, room.binarySnapshot));
            roomManager.setClientInitialStateReceived(roomId, clientId);

            console.log(`Client ${clientId} ${isReconnect ? 'reconnected to' : 'joined'} existing room ${roomId} (via CREATE_ROOM)`);
            break;
          }

          // Create room locally FIRST to prevent race with concurrent JOIN_ROOM
          room = roomManager.createRoom(roomId, snapshot, [], true);
          // Merge user from message with JWT metadata for disconnect events
          const creatorMetadata = { ...clientMetadata, ...user };
          roomManager.addClient(roomId, clientId, socket, nodeId, isMuted, creatorMetadata);
          checkClientSimVersion(roomId, clientId, creatorMetadata);
          currentRoomId = roomId;

          // Add room creator as a member
          const creatorOdId = authenticatedUserId || user?.id;
          memberOdId = creatorOdId;
          if (creatorOdId) {
            roomManager.addMember(roomId, creatorOdId, clientId, user);
          }

          // CRITICAL: Register with central service BEFORE sending response
          // This prevents race condition where second client asks Central before room is registered
          try {
            const response = await fetch(`${centralServiceUrl}/api/rooms/${roomId}/connect`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ creatorNodeId: nodeId })
            });

            if (!response.ok) {
              console.error(`Failed to register room with CS: ${response.status}`);
            }
          } catch (err) {
            console.error(`Error registering room with CS:`, err);
          }

          // CRITICAL: Start tick interval immediately so clients receive TICKs
          // Without this, clients wait forever for first TICK to run physics
          startRoomTick(roomId, peerManager);

          // SERVER-AUTHORITATIVE: Generate join input for room creator BEFORE sending response
          // This ensures the client receives their own join event in INITIAL_STATE
          const creatorJoinInput: NetworkInput = {
            id: generateInputId(),
            clientId,
            type: 'join',
            data: { type: 'join', clientId, user },
            seq: 0
            // frame is deliberately NOT set here. sendTick() stamps the frame the
            // input is actually broadcast in, which is the NEXT tick - stamping the
            // current frame on arrival makes room.inputs claim a frame no client has
            // received yet, and a late joiner rebuilding from that history replays
            // the input a tick early and desyncs permanently. `frame === undefined`
            // is also how every catch-up filter recognises "not broadcast yet".
            // See types.ts NetworkInput.frame.
          };

          const seq = roomManager.addInput(roomId, creatorJoinInput);
          if (seq) {
            creatorJoinInput.seq = seq;
            queueInputForPeers(roomId, creatorJoinInput, peerManager);
            queueInputForClients(roomId, creatorJoinInput, peerManager);
          }

          // Send ROOM_JOINED + INITIAL_STATE (consistent with JOIN_ROOM flow)
          //
          // Send the room's whole catch-up history, not just this client's own
          // join. A client is skipped by sendTick until initialStateReceived is
          // set, which happens on the line after this one - so anything that
          // lands in the room between creating it and sending this message
          // reaches the client through neither channel and is lost to it for
          // good. That is not hypothetical even for a "new" room: another node
          // may already be relaying joins for it (central assigns the authority
          // before the first client connects), and those joins arrive while this
          // handler is still running. The creator then never learns about a
          // player everyone else can see.
          //
          // selectCatchUpInputs always includes this client's own join, so the
          // original guarantee still holds.
          const currentFrame = getCurrentFrame(roomId);
          const creatorInputs = roomManager.selectCatchUpInputs(roomId, clientId);
          socket.send(encodeRoomJoined(roomId, clientId));
          socket.send(VOICE_READY_PACKET);
          socket.send(encodeInitialState(roomId, currentFrame, room.snapshot, room.snapshotHash, creatorInputs, room.binarySnapshot));
          roomManager.setClientInitialStateReceived(roomId, clientId);

          // Request replication to other nodes
          peerManager.requestReplication(roomId);

          // Authority: Sync master list to everyone (including creator)
          syncMasterClientList(roomId, room, peerManager);

          console.log(`Room created: ${roomId} by client ${clientId}`);
          break;
        }

        case MessageType.JOIN_ROOM: {
          const { roomId, user } = message.payload;
          const badJoin = badRoomId(roomId);
          if (badJoin) {
            console.error(`[JOIN_ROOM] Refusing ${clientId}: ${badJoin}`);
            socket.send(JSON.stringify({ type: MessageType.ERROR, payload: { message: badJoin } }));
            break;
          }
          if (!tokenAllowsRoom(tokenRoomId, roomId)) {
            console.error(`[SECURITY] ${clientId} presented a token for ${tokenRoomId} `
              + `and asked to join ${roomId}; refusing`);
            socket.send(JSON.stringify({
              type: MessageType.ERROR,
              payload: { message: 'this connection is not authorised for that room' },
            }));
            break;
          }
          console.log(`[JOIN_ROOM] Client ${clientId} joining room=${roomId}, user=${user?.id || 'unknown'}`);
          let room = roomManager.getRoom(roomId);

          if (!room) {
            // Room not in memory, check with centralized service
            const centralServiceUrl = process.env.CENTRAL_SERVICE_URL || 'http://localhost:9001';

            try {
              // Authenticated: a peer dial further down needs the credential
              // this returns, and central only mints one for a registered node.
              const response = await fetch(`${centralServiceUrl}/api/rooms/${roomId}`,
                { headers: selfAuthHeaders() });

              if (response.ok) {
                const roomData = await response.json() as any;
                const myNodeId = nodeId;

                const isAuthority = roomData.authorityNodeId === myNodeId;
                const isReplica = roomData.replicaNodeIds.includes(myNodeId);

                // Fetch state from authority node and create local replica
                if (isAuthority) {
                  // We are authority - but check if room was created while we awaited
                  room = roomManager.getRoom(roomId);
                  if (!room) {
                    room = roomManager.createRoom(roomId, { seq: 0 }, [], true);
                  }
                } else {
                  // We're not authority - fetch state from authority and become a replica
                  console.log(`[JOIN] Node ${myNodeId} is NOT authority for room ${roomId}, authority is ${roomData.authorityNodeId}`);
                  try {
                    const nodesResponse = await fetch(`${centralServiceUrl}/api/dashboard/stats`, { headers: selfAuthHeaders() });
                    if (nodesResponse.ok) {
                      const stats = await nodesResponse.json() as any;
                      const authorityNode = stats.nodes.find((n: any) => n.id === roomData.authorityNodeId);
                      console.log(`[JOIN] Looking for authority node ${roomData.authorityNodeId}, found: ${!!authorityNode}, available nodes: ${stats.nodes.map((n: any) => n.id).join(', ')}`);

                      if (authorityNode) {
                        // What the authority says its own API is, falling back to deriving it from
                    // the address clients dial. Those differ whenever the node is reached
                    // through a tunnel, and deriving it then points at the tunnel rather
                    // than at the node.
                    const authorityApiUrl = authorityNode.apiUrl || wsToHttp(authorityNode.connectionUrl);

                        // Establish the peer link BEFORE reading the authority's state.
                        //
                        // The other order leaves a hole: everything the authority
                        // broadcasts between the state read and the link coming up is
                        // sent to its current peers, which does not yet include us, and
                        // nothing ever backfills it. A join landing in that window is
                        // invisible to this node - and to its clients - for the rest of
                        // the room's life, which is how two nodes ended up disagreeing
                        // about who was in the room.
                        //
                        // Connecting first makes the window harmless instead: broadcasts
                        // that arrive before the room exists are held by
                        // bufferPeerHistory and folded in at creation, and anything
                        // after it applies directly. The state read then closes the gap
                        // at the other end by supplying everything from before the link.
                        console.log(`[JOIN] Connecting to authority peer ${roomData.authorityNodeId} before reading state`);
                        // Peer over the authority's own address when it has told us one. The
                        // client-facing URL may be a tunnel, and a tunnel answers as the
                        // service in front of the node rather than as the node.
                        await peerManager.connectToPeer(
                          roomData.authorityNodeId,
                          authorityNode.apiUrl ? peerUrlFromApi(authorityNode.apiUrl) : authorityNode.connectionUrl,
                          roomData.peerToken,
                        );

                        console.log(`[JOIN] Fetching state from authority at ${authorityApiUrl}/api/rooms/${roomId}/state`);

                        // ALWAYS fetch initial state via HTTP from authority - this is reliable and synchronous
                        // The authority stores all inputs (including joins) in room.inputs
                        const stateResponse = await fetch(`${authorityApiUrl}/api/rooms/${roomId}/state`);

                        if (stateResponse.ok) {
                          const authorityState = await stateResponse.json() as any;
                          console.log(`[JOIN] Got state from authority: frame=${authorityState.frame}, inputs=${authorityState.inputs?.length || authorityState.events?.length || 0}, snapshot.seq=${authorityState.snapshot?.seq}`);

                          // Check if room already exists (from SYNC_ROOM_STATE or REPLICATE_ROOM)
                          room = roomManager.getRoom(roomId);
                          if (room) {
                            // Room exists - merge any missing inputs from authority
                            const existingSeqs = new Set(room.inputs.map((e: any) => e.seq));
                            const newInputs = (authorityState.inputs || authorityState.events || []).filter((e: any) => !existingSeqs.has(e.seq));
                            if (newInputs.length > 0) {
                              room.inputs.push(...newInputs);
                              room.inputs.sort((a: any, b: any) => a.seq - b.seq);
                            }
                          } else {
                            // Create new room with authority's state
                            room = roomManager.createRoom(
                              roomId,
                              authorityState.snapshot,
                              authorityState.inputs || authorityState.events || [],
                              false // isAuthority = false
                            );
                          }

                          if (authorityState.frame !== undefined) {
                            setCurrentFrame(roomId, authorityState.frame);
                          }
                        } else {
                          console.log(`[JOIN] ERROR: Failed to fetch state from authority, status=${stateResponse.status}`);
                          room = roomManager.getRoom(roomId);
                          if (!room) {
                            console.log(`[JOIN] Creating EMPTY room as fallback - THIS WILL CAUSE DESYNC!`);
                            room = roomManager.createRoom(roomId, { seq: 0 }, [], false);
                          }
                        }

                        // Relay any buffered inputs to authority
                        const pendingInputs = roomManager.consumePendingRelayInputs(roomId);
                        if (pendingInputs.length > 0) {
                          console.log(`[BUFFER] Relaying ${pendingInputs.length} buffered inputs to authority for room ${roomId}`);
                          for (const input of pendingInputs) {
                            peerManager.broadcastToPeers({
                              type: MessageType.RELAY_INPUT,
                              payload: { roomId, input }
                            });
                          }
                        }
                      } else {
                        // Authority node is dead/stale - take over as authority
                        console.log(`[JOIN] WARNING: Authority node ${roomData.authorityNodeId} not found in stats! Taking over as authority with EMPTY state.`);
                        room = roomManager.getRoom(roomId);
                        if (!room) {
                          console.log(`[JOIN] Creating EMPTY room as new authority - THIS WILL CAUSE DESYNC!`);
                          room = roomManager.createRoom(roomId, { seq: 0 }, [], true);
                        } else {
                          // Room exists, upgrade to authority
                          roomManager.setAuthority(roomId, true);
                        }

                        // Notify central service about the authority change
                        try {
                          await fetch(`${centralServiceUrl}/api/rooms/${roomId}/authority`, {
                            method: 'PUT',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ authorityNodeId: myNodeId })
                          });
                        } catch (e) {
                          console.error(`Failed to update authority in central service:`, e);
                        }
                      }
                    } else {
                      room = roomManager.getRoom(roomId);
                      if (!room) {
                        room = roomManager.createRoom(roomId, { seq: 0 }, [], false);
                      }
                    }
                  } catch (fetchErr) {
                    console.error(`Error fetching authority state:`, fetchErr);
                    room = roomManager.getRoom(roomId);
                    if (!room) {
                      room = roomManager.createRoom(roomId, { seq: 0 }, [], false);
                    }
                  }
                }
              }
            } catch (err) {
              console.error('Failed to fetch room info:', err);
            }
          }

          if (!room) {
            socket.send(encodeError('Room not found'));
            return;
          }

          // CRITICAL FIX: For replica nodes, always fetch fresh state from authority
          // This prevents race conditions where REPLICATE_ROOM arrives before events are broadcast
          if (!room.isAuthority) {
            console.log(`[JOIN] Room ${roomId} is a replica - fetching fresh state from authority`);
            const centralServiceUrl = process.env.CENTRAL_SERVICE_URL || 'http://localhost:9001';

            // Retry up to 3 times with small delays to handle race with event processing
            const MAX_RETRIES = 3;
            const RETRY_DELAY_MS = 100;

            for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
              console.log(`[JOIN] Retry loop attempt ${attempt + 1} of ${MAX_RETRIES}`);
              try {
                const roomResponse = await fetch(`${centralServiceUrl}/api/rooms/${roomId}`);
                console.log(`[JOIN] Central service lookup (attempt ${attempt + 1}): status=${roomResponse.status}`);
                if (!roomResponse.ok) {
                  console.log(`[JOIN] Central service doesn't have room yet, will retry...`);
                  await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS));
                  continue;
                }
                if (roomResponse.ok) {
                  const roomData = await roomResponse.json() as any;
                  const statsResponse = await fetch(`${centralServiceUrl}/api/dashboard/stats`, { headers: selfAuthHeaders() });
                  if (statsResponse.ok) {
                    const stats = await statsResponse.json() as any;
                    const authorityNode = stats.nodes.find((n: any) => n.id === roomData.authorityNodeId);

                    if (authorityNode) {
                      // What the authority says its own API is, falling back to deriving it from
                    // the address clients dial. Those differ whenever the node is reached
                    // through a tunnel, and deriving it then points at the tunnel rather
                    // than at the node.
                    const authorityApiUrl = authorityNode.apiUrl || wsToHttp(authorityNode.connectionUrl);

                      const stateResponse = await fetch(`${authorityApiUrl}/api/rooms/${roomId}/state`);
                      if (stateResponse.ok) {
                        const authorityState = await stateResponse.json() as any;
                        const inputCount = authorityState.inputs?.length || authorityState.events?.length || 0;
                        console.log(`[JOIN] Got state from authority (attempt ${attempt + 1}): frame=${authorityState.frame}, inputs=${inputCount}`);

                        // If authority has no inputs yet and we have retries left, wait and retry
                        if (inputCount === 0 && attempt < MAX_RETRIES - 1) {
                          console.log(`[JOIN] Authority has no inputs yet, retrying in ${RETRY_DELAY_MS}ms...`);
                          await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS));
                          continue;
                        }

                        // Merge authority state into local room
                        const existingSeqs = new Set(room.inputs.map((e: any) => e.seq));
                        const newInputs = (authorityState.inputs || authorityState.events || []).filter((e: any) => !existingSeqs.has(e.seq));
                        if (newInputs.length > 0) {
                          room.inputs.push(...newInputs);
                          room.inputs.sort((a: any, b: any) => a.seq - b.seq);
                          console.log(`[JOIN] Merged ${newInputs.length} new inputs from authority`);
                        }

                        // Update frame if authority is ahead
                        if (authorityState.frame !== undefined) {
                          const localFrame = getCurrentFrame(roomId);
                          if (authorityState.frame > localFrame) {
                            setCurrentFrame(roomId, authorityState.frame);
                          }
                        }
                        break; // Success - exit retry loop
                      }
                    }
                  }
                }
              } catch (err) {
                console.error(`[JOIN] Error fetching state from authority (attempt ${attempt + 1}):`, err);
              }

              // Wait before retry
              if (attempt < MAX_RETRIES - 1) {
                console.log(`[JOIN] Waiting ${RETRY_DELAY_MS}ms before retry...`);
                await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS));
              }
            }
            console.log(`[JOIN] Finished fetching from authority, local inputs: ${room.inputs.length}`);

            // CRITICAL: Establish peer connection to authority for real-time BROADCAST_INPUTS
            // Without this, replica nodes won't receive TICK messages to forward to clients
            try {
              const roomResponse = await fetch(`${centralServiceUrl}/api/rooms/${roomId}`,
                { headers: selfAuthHeaders() });
              if (roomResponse.ok) {
                const roomData = await roomResponse.json() as any;
                const statsResponse = await fetch(`${centralServiceUrl}/api/dashboard/stats`, { headers: selfAuthHeaders() });
                if (statsResponse.ok) {
                  const stats = await statsResponse.json() as any;
                  const authorityNode = stats.nodes.find((n: any) => n.id === roomData.authorityNodeId);
                  if (authorityNode) {
                    console.log(`[JOIN] Connecting to authority peer ${roomData.authorityNodeId} for real-time updates`);
                    // Peer over the authority's own address when it has told us one. The
                        // client-facing URL may be a tunnel, and a tunnel answers as the
                        // service in front of the node rather than as the node.
                        await peerManager.connectToPeer(
                          roomData.authorityNodeId,
                          authorityNode.apiUrl ? peerUrlFromApi(authorityNode.apiUrl) : authorityNode.connectionUrl,
                          roomData.peerToken,
                        );
                  }
                }
              }
            } catch (peerErr) {
              console.error(`[JOIN] Error connecting to authority peer:`, peerErr);
            }
          }

          // Merge user from JOIN_ROOM with JWT metadata for disconnect events
          const mergedMetadata = { ...user, ...clientMetadata };
          roomManager.addClient(roomId, clientId, socket, nodeId, isMuted, mergedMetadata);
          checkClientSimVersion(roomId, clientId, mergedMetadata);
          currentRoomId = roomId;

          // Check if this is a reconnecting member (user.id already exists in members)
          const odId = authenticatedUserId || user?.id;
          memberOdId = odId;
          const existingMember = odId ? roomManager.getMemberByOdId(roomId, odId) : undefined;
          const isReconnect = existingMember && existingMember.status === 'disconnected';

          // SERVER-AUTHORITATIVE: Generate join input FIRST before sending INITIAL_STATE
          // This allows authority to upload fresh snapshot before we capture it
          const inputType = isReconnect ? 'reconnect' : 'join';
          const connectionInput: NetworkInput = {
            id: generateInputId(),
            clientId,
            type: inputType,
            data: { type: inputType, clientId, user },
            seq: 0
            // frame is deliberately NOT set here. sendTick() stamps the frame the
            // input is actually broadcast in, which is the NEXT tick - stamping the
            // current frame on arrival makes room.inputs claim a frame no client has
            // received yet, and a late joiner rebuilding from that history replays
            // the input a tick early and desyncs permanently. `frame === undefined`
            // is also how every catch-up filter recognises "not broadcast yet".
            // See types.ts NetworkInput.frame.
          };

          // Update membership
          if (odId && existingMember) {
            // See the CREATE_ROOM path: the member names its newest connection
            // whether or not it had been marked disconnected first.
            roomManager.setMemberStatus(roomId, odId, 'connected', clientId);
            console.log(
              `[${isReconnect ? 'RECONNECT' : 'REJOIN'}] Member ${odId} is now client ${clientId}`
            );
          } else if (odId) {
            roomManager.addMember(roomId, odId, clientId, user);
          }

          // If this is a replica node, notify authority of new client
          if (!room.isAuthority) {
            peerManager.broadcastToPeers({
              type: MessageType.RELAY_CLIENT_JOIN,
              payload: { roomId, clientId, nodeId, isMuted, metadata: mergedMetadata }
            });
            console.log(`[MASTER_CLIENT_LIST] Sent RELAY_CLIENT_JOIN for ${clientId} to peers (authority will handle)`);
          }

          // Process join and broadcast to existing clients (triggers authority to upload fresh snapshot)
          if (room.isAuthority) {
            const seq = roomManager.addInput(roomId, connectionInput);
            if (seq) {
              connectionInput.seq = seq;
              queueInputForPeers(roomId, connectionInput, peerManager);
              queueInputForClients(roomId, connectionInput, peerManager);
            }
            syncMasterClientList(roomId, room, peerManager);

            // CRITICAL: Wait for authority client to upload snapshot that INCLUDES this join
            // We must wait for snapshot.seq >= joinSeq, not just snapshot.seq > oldSeq!
            // Otherwise we may send a stale snapshot that doesn't have the new player's entity.
            const joinSeq = connectionInput.seq || 0;
            const otherClientsExist = room.clients.size > 1;  // > 1 because new client was just added
            if (otherClientsExist || joinSeq > 0) {
              const SNAPSHOT_WAIT_MS = 500;
              const POLL_INTERVAL_MS = 15;
              const startTime = Date.now();
              console.log(`[JOIN] Waiting for snapshot with seq >= ${joinSeq} (joinSeq), otherClients=${room.clients.size}`);
              // When the server had to infer snapshot.seq it is a lower bound
              // bounded by the snapshot's own frame, so it can never reach
              // joinSeq and this loop would burn its full timeout on every
              // single join. Wait on the frame in that case, which is the same
              // question asked in the units the snapshot actually carries.
              const joinFrame = getCurrentFrame(roomId);
              while (Date.now() - startTime < SNAPSHOT_WAIT_MS) {
                if (roomManager.isSnapshotSeqInferred(roomId)) {
                  const snapFrame = roomManager.getSnapshotFrame(roomId);
                  if (snapFrame >= joinFrame) {
                    console.log(`[JOIN] Fresh snapshot received: frame=${snapFrame} >= joinFrame=${joinFrame}`);
                    break;
                  }
                } else {
                  const currentSnapshotSeq = room.snapshot?.seq || 0;
                  // CRITICAL: Wait for snapshot that includes this join (seq >= joinSeq)
                  if (currentSnapshotSeq >= joinSeq) {
                    console.log(`[JOIN] Fresh snapshot received: seq=${currentSnapshotSeq} >= joinSeq=${joinSeq}`);
                    break;
                  }
                }
                await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL_MS));
              }

              // CRITICAL: After snapshot arrives, wait for a few more ticks (150ms = ~3 ticks @ 20fps)
              // This ensures game inputs for catchup frames are received and stored in room.inputs
              // Without this, late joiner runs catchup with 0 inputs → divergence
              const POST_SNAPSHOT_WAIT_MS = 150;
              console.log(`[JOIN] Waiting ${POST_SNAPSHOT_WAIT_MS}ms for game inputs to accumulate...`);
              await new Promise(resolve => setTimeout(resolve, POST_SNAPSHOT_WAIT_MS));
              console.log(`[JOIN] After wait: room.inputs.length=${room.inputs.length}`);
            }
          } else {
            // REPLICA: Relay join/reconnect input to authority via HTTP (reliable, synchronous)
            const centralServiceUrl = process.env.CENTRAL_SERVICE_URL || 'http://localhost:9001';
            let relayed = false;
            try {
              // Authenticated: the credential for the relay below comes back on
              // this call, which this path was making anyway to find the
              // authority. Asking central for one separately would put a round
              // trip in front of every relay.
              const roomResponse = await fetch(`${centralServiceUrl}/api/rooms/${roomId}`,
                { headers: selfAuthHeaders() });
              if (roomResponse.ok) {
                const roomData = await roomResponse.json() as any;
                const statsResponse = await fetch(`${centralServiceUrl}/api/dashboard/stats`, { headers: selfAuthHeaders() });
                if (statsResponse.ok) {
                  const stats = await statsResponse.json() as any;
                  const authorityNode = stats.nodes.find((n: any) => n.id === roomData.authorityNodeId);
                  if (authorityNode) {
                    // What the authority says its own API is, falling back to deriving it from
                    // the address clients dial. Those differ whenever the node is reached
                    // through a tunnel, and deriving it then points at the tunnel rather
                    // than at the node.
                    const authorityApiUrl = authorityNode.apiUrl || wsToHttp(authorityNode.connectionUrl);
                    const relayResponse = await fetch(`${authorityApiUrl}/api/rooms/${roomId}/relay`, {
                      method: 'POST',
                      headers: {
                        'Content-Type': 'application/json',
                        Authorization: `Bearer ${roomData.relayToken}`
                      },
                      body: JSON.stringify({ input: connectionInput })
                    });
                    if (!relayResponse.ok) {
                      console.error(`[JOIN] Failed to relay ${inputType} input via HTTP: ${relayResponse.status}`);
                      relayed = false;
                    } else {
                      relayed = true;
                    }
                  } else {
                    console.error(`[JOIN] Authority node ${roomData.authorityNodeId} not found in central's node list - cannot relay ${inputType}`);
                  }
                } else {
                  console.error(`[JOIN] Could not read node list from central (${statsResponse.status}) - cannot relay ${inputType}`);
                }
              } else {
                console.error(`[JOIN] Could not read room ${roomId} from central (${roomResponse.status}) - cannot relay ${inputType}`);
              }
            } catch (err) {
              console.error(`[JOIN] Error relaying ${inputType} input via HTTP:`, err);
            }

            // Every failure above used to be a silent no-op or a log with no
            // recovery, which meant this client's own join never reached the
            // authority and therefore never reached anyone else: the room simply
            // never learned the player existed. Fall back to the peer link, which
            // by this point is established.
            if (!relayed) {
              console.error(`[JOIN] HTTP relay of ${inputType} did not succeed - falling back to the peer link`);
              peerManager.broadcastToPeers({
                type: MessageType.RELAY_INPUT,
                payload: { roomId, input: connectionInput }
              });
            }

            // Simple fix: if replica has no inputs, wait a bit for broadcasts to arrive
            if (room.inputs.length === 0) {
              console.log(`[JOIN] Replica has 0 inputs, waiting 200ms for broadcasts...`);
              await new Promise(resolve => setTimeout(resolve, 200));
              console.log(`[JOIN] After wait, inputs: ${room.inputs.length}`);
            }
          }

          // NOW capture snapshot AFTER waiting for fresh snapshot from authority
          const snapshotData = room.snapshot;
          const snapshotHashData = room.snapshotHash;
          const snapshotSeq = snapshotData?.seq || 0;
          console.log(`[JOIN] Snapshot data: frame=${snapshotData?.frame}, seq=${snapshotSeq}, hash=${snapshotHashData}`);


          // Pick the history the late joiner replays on top of the snapshot.
          // See roomManager.selectCatchUpInputs for the seq-vs-frame rule and why
          // guessing high silently desyncs the joiner.
          //
          // Regression coverage: e2e/late-join.spec.ts (no seq, has frame) and
          // tests/test-late-join-sync.ts (explicit seq).
          const inputsSnapshot = roomManager.selectCatchUpInputs(roomId, clientId);

          console.log(`[INITIAL_STATE] Total inputs: ${inputsSnapshot.length} (seq > ${snapshotSeq})`);

          // Send snapshot and inputs to late joiner
          const currentFrame = getCurrentFrame(roomId);
          if (process.env.TICK_TRACE) {
            console.log(`[JOIN-TRACE] client=${clientId.slice(0, 8)} currentFrame=${currentFrame} snapshotFrame=${roomManager.getSnapshotFrame(roomId)} seqInferred=${roomManager.isSnapshotSeqInferred(roomId)} snapshotSeq=${snapshotSeq} inputs=${inputsSnapshot.map((i: any) => `${i.seq}@${i.frame}`).join(',')}`);
          }
          console.log(`[INITIAL_STATE] Sending to ${clientId}: inputs=${inputsSnapshot.length} (filtered from ${room.inputs.length}, snapshotSeq=${snapshotSeq}), frame=${currentFrame}, isAuthority=${room.isAuthority}`);

          // Send initial state to client with FRESH snapshot
          socket.send(encodeRoomJoined(roomId, clientId));
          socket.send(VOICE_READY_PACKET);
          socket.send(encodeInitialState(roomId, currentFrame, snapshotData, snapshotHashData, inputsSnapshot, room.binarySnapshot));
          roomManager.setClientInitialStateReceived(roomId, clientId);

          console.log(`Client ${clientId} ${isReconnect ? 'reconnected to' : 'joined'} room ${roomId}`);
          break;
        }

        case MessageType.LEAVE_ROOM: {
          if (currentRoomId) {
            const room = roomManager.getRoom(currentRoomId);
            const clientInfo = room?.clients.get(clientId);
            // The member this connection joined as - see memberOdId. The
            // client's own metadata is a fallback for a connection that somehow
            // never recorded one, not the answer.
            const odId = memberOdId || clientInfo?.metadata?.id;

            // Generate and broadcast "leave" input (permanent departure)
            const leaveInput: NetworkInput = {
              id: generateInputId(),
              clientId,
              type: 'leave',
              data: { type: 'leave', clientId, user: clientInfo?.metadata },
              seq: 0
              // frame is deliberately NOT set here. sendTick() stamps the frame the
              // input is actually broadcast in, which is the NEXT tick - stamping the
              // current frame on arrival makes room.inputs claim a frame no client has
              // received yet, and a late joiner rebuilding from that history replays
              // the input a tick early and desyncs permanently. `frame === undefined`
              // is also how every catch-up filter recognises "not broadcast yet".
              // See types.ts NetworkInput.frame.
            };

            if (room?.isAuthority) {
              const seq = roomManager.addInput(currentRoomId, leaveInput);
              if (seq) {
                leaveInput.seq = seq;
                queueInputForPeers(currentRoomId, leaveInput, peerManager);
                queueInputForClients(currentRoomId, leaveInput, peerManager);
              }
            } else if (room) {
              // Relay to authority
              peerManager.broadcastToPeers({
                type: MessageType.RELAY_INPUT,
                payload: { roomId: currentRoomId, input: leaveInput }
              });
            }

            // Remove from members (permanent departure) and clients
            if (odId) {
              roomManager.removeMember(currentRoomId, odId);
            }
            roomManager.removeClient(currentRoomId, clientId);
            voiceRelay.forget(currentRoomId, clientId);
            // Remove client from partition collector to stop their hash from polluting majority
            roomManager.removeClientFromCollector(currentRoomId, clientId);

            // If this is a replica node, notify authority of client leaving
            if (room && !room.isAuthority) {
              peerManager.broadcastToPeers({
                type: MessageType.RELAY_CLIENT_LEAVE,
                // `permanent` because this one really is a departure. The same
                // message is sent when a socket merely closes, and the authority
                // must not confuse the two: see applyRelayClientLeave.
                payload: { roomId: currentRoomId, clientId, permanent: true, user: clientInfo?.metadata }
              });
              console.log(`[MASTER_CLIENT_LIST] Sent RELAY_CLIENT_LEAVE for ${clientId} to peers (authority will handle)`);
            } else if (room && room.isAuthority) {
              // Authority: Sync master list to everyone
              syncMasterClientList(currentRoomId, room, peerManager);
            }

            socket.send(encodeRoomLeft(currentRoomId));

            console.log(`Client ${clientId} (member ${odId}) left room ${currentRoomId}`);
            currentRoomId = undefined;
          }
          break;
        }

        case MessageType.SEND_EVENT:
        case MessageType.SEND_INPUT: {
          const { data } = message.payload;

          // The room this connection is in, not the room its payload names.
          //
          // Every input carried a roomId and the server used it, so a client
          // could put inputs into any room on the node just by naming it -
          // including rooms it had never joined and could not have joined.
          // Demonstrated against a quiet room: a connection joined to one room
          // sent six inputs naming another, and the client sitting in that
          // other room received all six.
          //
          // Nothing downstream saved it. The inputs were sequenced, given
          // sequence numbers, broadcast to everybody in the target room and
          // written into its history. What kept the demos intact is that they
          // attribute an input to the connection the server stamped on it, so
          // an outsider is not a player and their input does nothing - an
          // application that trusts what the payload says about itself has no
          // such protection, and none of them should have to.
          //
          // A connection belongs to exactly one room, and the node has known
          // which since the join. The payload's copy is now only checked
          // against it.
          const claimedRoomId = message.payload?.roomId;
          if (!currentRoomId || (claimedRoomId && claimedRoomId !== currentRoomId)) {
            console.error(`[SECURITY] ${clientId} is in ${currentRoomId ?? 'no room'} `
              + `and sent an input for ${claimedRoomId}; refusing`);
            break;
          }
          const roomId = currentRoomId;

          if (isWriteBlocked('input')) break;

          // Diagnostics are exempt: they are already rate limited on the client
          // and never enter the replicated stream.
          if (data?.type !== 'desync_report' && overInputBudget(clientId)) break;

          // Size, judged on what will actually be replicated. A desync report
          // is exempt from the rate limit but not from this: it is still a
          // message this node has to hold and parse.
          if (oversizedInput(clientId, jsonInputBytes(data), 'input')) break;

          // A desync report is diagnostics, not gameplay. Record it and stop -
          // letting it through would put a message every client must ignore into
          // the replicated input stream, and burn a sequence number doing it.
          if (data?.type === 'desync_report') {
            roomManager.recordDesyncReport(roomId, {
              clientId,
              receivedAt: Date.now(),
              frame: Number(data.frame) || 0,
              mine: Number(data.mine) >>> 0,
              majority: Number(data.majority) >>> 0,
              player: data.player,
              app: data.app,
              simFrames: data.simFrames,
              inputsApplied: data.inputsApplied,
              restoredFrom: data.restoredFrom,
              status: data.status,
            });
            console.error(
              `[DESYNC] room=${roomId} client=${clientId.slice(0, 8)} player=${data.player} ` +
              `frame=${data.frame} mine=${(Number(data.mine) >>> 0).toString(16)} ` +
              `majority=${(Number(data.majority) >>> 0).toString(16)} ` +
              `app=${data.app} simFrames=${data.simFrames} restoredFrom=${data.restoredFrom}`
            );
            break;
          }

          // SECURITY: Reject client-sent join/leave inputs - these are server-authoritative
          if (data?.type === 'join' || data?.type === 'leave' || data?.type === 'disconnect' || data?.type === 'reconnect') {
            console.log(`[SECURITY] Rejecting client-sent ${data.type} input from ${clientId} - use JOIN_ROOM/disconnect instead`);
            break;
          }

          console.log(`[INPUT] Received from client ${clientId}, type=${data?.type}, room=${roomId}`);
          let room = roomManager.getRoom(roomId);

          const input: NetworkInput = {
            id: generateInputId(),
            clientId,
            type: data?.type || 'input',
            data: data, // Keep as-is, can be any JSON-serializable data
            seq: 0, // Will be set by authority node
            // frame is deliberately NOT set here. sendTick() stamps the frame the
            // input is actually broadcast in, which is the NEXT tick - stamping the
            // current frame on arrival makes room.inputs claim a frame no client has
            // received yet, and a late joiner rebuilding from that history replays
            // the input a tick early and desyncs permanently. `frame === undefined`
            // is also how every catch-up filter recognises "not broadcast yet".
            // See types.ts NetworkInput.frame.
          };

          // If room doesn't exist yet, buffer the input for when it appears
          if (!room) {
            console.log(`[INPUT] Room ${roomId} not found, buffering input type=${data?.type}`);
            roomManager.bufferInput(roomId, input);
            break;
          }

          // NOTE: input.clientId identifies who sent this input (server-authoritative)
          // Application layer can use this to identify the sender

          if (room.isAuthority) {
            // This node is authority, process immediately
            const seq = roomManager.addInput(roomId, input);

            if (seq) {
              input.seq = seq;

              // Queue for batched broadcast to peers and clients
              queueInputForPeers(roomId, input, peerManager);
              queueInputForClients(roomId, input, peerManager);
            }
          } else {
            // Relay to authority node through peers
            const peers = peerManager.getPeers().filter(p => p.isConnected);
            console.log(`[NON-AUTH] Relaying input type=${input.data?.type || 'unknown'} for room ${roomId} to ${peers.length} peers`);
            if (peers.length === 0) {
              console.log(`[NON-AUTH] WARNING: No connected peers to relay to!`);
            }
            peerManager.broadcastToPeers({
              type: MessageType.RELAY_INPUT,
              payload: { roomId, input }
            });
          }
          break;
        }

        case MessageType.SEND_SNAPSHOT: {
          const { snapshot, hash } = message.payload;

          // The connection's room, for the same reason as inputs - and this one
          // matters more.
          //
          // A snapshot is what every late joiner restores from, so whoever
          // writes it defines the world new clients begin in. This took the
          // room from the payload, so a connection could write the snapshot of
          // a room it had never joined. Demonstrated against a quiet room: an
          // outsider published `{"state":{"POISONED":"from-another-room"}}` and
          // the room stored it, hash and all.
          //
          // snapshotDisagreesWithRoom does not stop it. That refuses a
          // publisher the room has already judged out of step, and an outsider
          // has never voted there at all - there is nothing to judge it
          // against, so it passes.
          const claimedRoomId = message.payload?.roomId;
          if (!currentRoomId || (claimedRoomId && claimedRoomId !== currentRoomId)) {
            console.error(`[SECURITY] ${clientId} is in ${currentRoomId ?? 'no room'} `
              + `and published a snapshot for ${claimedRoomId}; refusing`);
            break;
          }
          const roomId = currentRoomId;
          if (isWriteBlocked('snapshot')) break;
          const room = roomManager.getRoom(roomId);
          console.log(`[SNAPSHOT] Received from ${clientId} for room ${roomId}, seq: ${snapshot?.seq}, frame=${snapshot?.frame}, currentInputs: ${room?.inputs?.length || 0}`);

          // Refuse a snapshot of a world the room does not agree with.
          //
          // This is what late joiners restore from and what a resync hands a
          // client asking to be repaired, so accepting it from a client that
          // has diverged spreads one client's broken world to everyone who
          // arrives afterwards - and to the diverged client itself, which is
          // then repaired with its own corruption and never recovers.
          if (roomManager.snapshotDisagreesWithRoom(roomId, clientId, snapshot?.frame, hash)) {
            console.warn(`[SNAPSHOT] Refused from ${clientId.slice(0, 8)} for room ${roomId} at frame ${snapshot?.frame}:`
              + ` its hash ${hash} is not what the room agreed for that frame`);
            break;
          }

          // Validate snapshot hash
          if (roomManager.validateSnapshotHash(snapshot, hash)) {
            roomManager.updateSnapshot(roomId, snapshot, hash);

            console.log(`[SNAPSHOT] Updated room ${roomId}`);
            roomManager.noteSnapshotPublisher(roomId, clientId);

            // Send resync snapshot to any clients waiting for it
            if (room && room.pendingResyncClients.size > 0) {
              const currentFrame = getCurrentFrame(roomId);

              for (const resyncClientId of room.pendingResyncClients) {
                const clientInfo = room.clients.get(resyncClientId);
                if (clientInfo?.socket) {
                  // Selected per client, like every other catch-up path.
                  //
                  // This used to compute one set with no client id and send it
                  // to everybody waiting, which skipped all three of the rules
                  // that make a catch-up usable: the client's own join, the
                  // joins of everyone already in the room, and the joins that
                  // explain the inputs being sent. A client that cannot
                  // attribute an input drops it while everyone else applies it,
                  // so the repair could leave a client diverged in exactly the
                  // way it was called on to fix - and this is the path of last
                  // resort, so there is nothing after it to catch that.
                  const inputsSnapshot = roomManager.selectCatchUpInputs(roomId, resyncClientId);
                  console.log(`[RESYNC] Sending snapshot to ${resyncClientId.slice(0, 8)} (frame=${currentFrame}, inputs=${inputsSnapshot.length})`);
                  clientInfo.socket.send(encodeInitialState(roomId, currentFrame, snapshot, hash, inputsSnapshot, room.binarySnapshot));
                }
              }
              room.pendingResyncClients.clear();
            }

            // DO NOT broadcast snapshots to other clients - determinism means they don't need it
            // Snapshots are only for late joiners (stored on server) and peer sync
            // Broadcasting to clients would violate the "no drift correction" rule

            // Broadcast snapshot update to peers
            // Only include inputs SINCE the snapshot (seq > snapshot.seq)
            // Late joiners need snapshot + inputs after, not full history
            const updatedRoom = roomManager.getRoom(roomId);
            const snapshotSeq = snapshot?.seq || 0;
            const inputsSinceSnapshot = (updatedRoom?.inputs || []).filter(
              (e: any) => e.seq > snapshotSeq
            );
            console.log(`[SNAPSHOT] Broadcasting to peers: seq=${snapshotSeq}, inputsSince=${inputsSinceSnapshot.length} (total=${updatedRoom?.inputs?.length || 0})`);
            peerManager.broadcastToPeers({
              type: MessageType.SYNC_ROOM_STATE,
              payload: {
                rooms: [{
                  id: roomId,
                  snapshot,
                  snapshotHash: hash,
                  inputs: inputsSinceSnapshot,
                  events: inputsSinceSnapshot // Backwards compatibility
                }]
              }
            });
          } else {
            socket.send(encodeError('Invalid snapshot hash'));
          }
          break;
        }

        /**
         * The page is unloading: closing, or reloading and about to come back.
         *
         * Membership deliberately survives a dropped socket so that a refresh
         * or a flaky network resumes rather than restarts, and nothing in a
         * closed socket says which of those happened - so every departure was
         * held for the full grace, and a player who quit the demo stood in the
         * arena, on the leaderboard, for two minutes.
         *
         * The browser knows what the socket cannot. An unload is followed
         * within seconds either by nothing at all or by the same member
         * rejoining, so a member that announced one is expired on the short
         * grace. A socket that dies without this still gets the long one,
         * which is the case that grace was written for.
         */
        case MessageType.GOING_AWAY: {
          if (currentRoomId && memberOdId) {
            roomManager.setMemberUnloading(currentRoomId, memberOdId);
            console.log(`[UNLOAD] ${memberOdId} in ${currentRoomId} says its page is going away`);
          }
          break;
        }

        case MessageType.GET_CLIENTS: {
          // Same rule. Who is in a room is the room's business, and this
          // answered for any room that was named.
          const claimedClientsRoom = message.payload?.roomId;
          if (!currentRoomId || (claimedClientsRoom && claimedClientsRoom !== currentRoomId)) {
            console.error(`[SECURITY] ${clientId} is in ${currentRoomId ?? 'no room'} `
              + `and asked for the client list of ${claimedClientsRoom}; refusing`);
            break;
          }
          const roomId = currentRoomId;
          const room = roomManager.getRoom(roomId);
          if (!room) {
            socket.send(encodeError('Room not found'));
            break;
          }

          const masterList = room.masterClientList;
          const clientListArray = masterList ? Array.from(masterList.values()).map(c => ({
            clientId: c.clientId,
            metadata: c.metadata,
            isMuted: c.isMuted
          })) : [];

          socket.send(encodeClientListUpdate(roomId, clientListArray));
          break;
        }

        case MessageType.REQUEST_RESYNC: {
          // The room this connection is in. A resync hands back the room's
          // whole world, so naming somebody else's room here read it out to
          // anyone who asked: demonstrated by an outsider requesting a resync
          // for a room it had never joined and receiving that room's state,
          // secret and all. For a chat application that is the message
          // history; for a game it is the board.
          const claimedResyncRoom = message.payload?.roomId;
          if (!currentRoomId || (claimedResyncRoom && claimedResyncRoom !== currentRoomId)) {
            console.error(`[SECURITY] ${clientId} is in ${currentRoomId ?? 'no room'} `
              + `and asked to resync ${claimedResyncRoom}; refusing`);
            socket.send(encodeError('not in that room'));
            break;
          }
          const roomId = currentRoomId;
          console.log(`[RESYNC] Client ${clientId.slice(0, 8)} requested resync for room ${roomId}`);
          const room = roomManager.getRoom(roomId);
          if (!room) {
            socket.send(encodeError('Room not found'));
            break;
          }

          // Immediately send the stored snapshot to the requesting client
          // Don't wait for authority - they may not respond quickly during rapid refresh
          //
          // Selected the same way as every other catch-up. This built its own
          // filter, which is how it came to be the one path that skipped all
          // three rules that make a catch-up usable: the client's own join, the
          // joins of everyone already in the room, and the joins that explain
          // the inputs being sent. A client that cannot attribute an input
          // drops it while everyone else applies it, so the repair handed back
          // a world that was wrong in a new way.
          //
          // It showed as a resync that did not take. A deliberately corrupted
          // client was served this immediately, stayed broken, and only came
          // right several hundred frames later when a second client joined and
          // published a snapshot, which flushed the *other* resync path - the
          // one that does use the shared selection. Two paths, one correct, and
          // the wrong one answered first.
          const inputsSnapshot = roomManager.selectCatchUpInputs(roomId, clientId);
          const currentFrame = getCurrentFrame(roomId);

          console.log(`[RESYNC] Sending stored snapshot to ${clientId.slice(0, 8)} (frame=${currentFrame}, snapshotFrame=${room.snapshot?.frame}, inputs=${inputsSnapshot.length})`);
          socket.send(encodeInitialState(roomId, currentFrame, room.snapshot, room.snapshotHash, inputsSnapshot, room.binarySnapshot));

          // Also inject a "resync_request" input so authority uploads fresh snapshot for future requests
          const resyncInput: NetworkInput = {
            id: `resync-${clientId}-${Date.now()}`,
            clientId,
            type: 'json',
            data: { type: 'resync_request', clientId },
            seq: 0
            // frame is deliberately NOT set here. sendTick() stamps the frame the
            // input is actually broadcast in, which is the NEXT tick - stamping the
            // current frame on arrival makes room.inputs claim a frame no client has
            // received yet, and a late joiner rebuilding from that history replays
            // the input a tick early and desyncs permanently. `frame === undefined`
            // is also how every catch-up filter recognises "not broadcast yet".
            // See types.ts NetworkInput.frame.
          };

          // Sequence numbers are the authority's to hand out, and this was the
          // one injection site that did not check.
          //
          // Every other path guards on isAuthority and relays to the authority
          // otherwise; this one called addInput wherever it ran, so a replica
          // serving a resync minted a seq from its own counter. Two nodes then
          // number different inputs the same, which is exactly the divergence
          // measured while chasing the multi-node join bug - 104 of the
          // replica's sequence numbers did not exist on the authority at all.
          // A seq that means different things on different nodes breaks every
          // consumer of it: catch-up selection filters by it, the pruner drops
          // by it, and peers de-duplicate by it.
          if (room.isAuthority) {
            const seq = roomManager.addInput(roomId, resyncInput);
            if (seq) {
              resyncInput.seq = seq;
              queueInputForPeers(roomId, resyncInput, peerManager);
              queueInputForClients(roomId, resyncInput, peerManager);
            }
          } else {
            // Unsequenced, so the authority assigns one and broadcasts it back
            // to everyone including us. The peer handler skips inputs that
            // already carry a seq, so this cannot be double-numbered.
            peerManager.broadcastToPeers({
              type: MessageType.RELAY_INPUT,
              payload: { roomId, input: resyncInput }
            });
          }
          break;
        }

        default:
          socket.send(encodeError('Unknown message type'));
      }
    } catch (error) {
      console.error('Error handling client message:', error);
      socket.send(encodeError('Invalid message format'));
    }
  });

  socket.on('close', () => {
    clearInputBudget(clientId);

    if (currentRoomId) {
      const room = roomManager.getRoom(currentRoomId);

      // Get client metadata (stored when client joined)
      const clientInfo = room?.clients.get(clientId);
      // The member this connection joined as - see memberOdId.
      const odId = memberOdId || clientInfo?.metadata?.id;

      /**
       * Is this connection still the member's, or has a newer one taken over?
       *
       * A refresh opens the new socket while the old one is closing, and the
       * two are raced: the close is usually processed first, but under load the
       * new JOIN_ROOM can arrive before it. When that happened, this handler
       * marked a member as disconnected who was connected and playing - and
       * with a short grace behind it, the sweep then removed a live player from
       * the world fifteen seconds later. The long grace hid it rather than
       * fixing it: the member was equally wrong, for two minutes.
       *
       * A member names the connection that owns it, updated by every join. If
       * that is no longer this one, this close says nothing about the member -
       * only that an old socket of theirs has finished closing.
       */
      const member = odId ? roomManager.getMemberByOdId(currentRoomId, odId) : undefined;
      const stillOurs = !member || member.clientId === clientId;
      if (!stillOurs) {
        console.log(
          `[DISCONNECT] Client ${clientId} closed, but member ${odId} has since ` +
          `been claimed by ${member!.clientId}; leaving membership alone`
        );
      }

      // SERVER-AUTHORITATIVE: Generate "disconnect" input (NOT "leave")
      // Member stays in room, just marked as disconnected
      const disconnectInput: NetworkInput = {
        id: generateInputId(),
        clientId,
        type: 'disconnect',
        data: { type: 'disconnect', clientId, user: clientInfo?.metadata },
        seq: 0
        // frame is deliberately NOT set here. sendTick() stamps the frame the
        // input is actually broadcast in, which is the NEXT tick - stamping the
        // current frame on arrival makes room.inputs claim a frame no client has
        // received yet, and a late joiner rebuilding from that history replays
        // the input a tick early and desyncs permanently. `frame === undefined`
        // is also how every catch-up filter recognises "not broadcast yet".
        // See types.ts NetworkInput.frame.
      };

      console.log(`[DISCONNECT] Client ${clientId} (member ${odId}) disconnecting from room ${currentRoomId}`);

      // Announced only when the member really is disconnecting. A disconnect for
      // a member whose newer connection is live tells every client that somebody
      // playing has gone, which is both untrue and acted upon - it is what the
      // publisher election reads.
      if (!stillOurs) {
        // Nothing to announce and nothing to demote; the socket bookkeeping
        // below still runs.
      } else if (room && room.isAuthority) {
        // Authority: assign seq and broadcast
        const seq = roomManager.addInput(currentRoomId, disconnectInput);
        if (seq) {
          disconnectInput.seq = seq;
          console.log(`[DISCONNECT] Broadcasting disconnect for ${clientId} in room ${currentRoomId}, seq=${seq}`);
          queueInputForPeers(currentRoomId, disconnectInput, peerManager);
          queueInputForClients(currentRoomId, disconnectInput, peerManager);
        }
      } else if (room) {
        console.log(`[MASTER_CLIENT_LIST] Sent RELAY_CLIENT_LEAVE for ${clientId} to peers (authority will handle)`);
        // Replica: relay disconnect input to authority via HTTP (reliable, synchronous)
        console.log(`[DISCONNECT] Relaying disconnect for ${clientId} in room ${currentRoomId} to authority via HTTP`);
        const centralServiceUrl = process.env.CENTRAL_SERVICE_URL || 'http://localhost:9001';
        const roomId = currentRoomId;

        // Use async IIFE since close handler can't be async
        (async () => {
          try {
            const roomResponse = await fetch(`${centralServiceUrl}/api/rooms/${roomId}`,
              { headers: selfAuthHeaders() });
            if (roomResponse.ok) {
              const roomData = await roomResponse.json() as any;
              const statsResponse = await fetch(`${centralServiceUrl}/api/dashboard/stats`, { headers: selfAuthHeaders() });
              if (statsResponse.ok) {
                const stats = await statsResponse.json() as any;
                const authorityNode = stats.nodes.find((n: any) => n.id === roomData.authorityNodeId);
                if (authorityNode) {
                  // What the authority says its own API is, falling back to deriving it from
                    // the address clients dial. Those differ whenever the node is reached
                    // through a tunnel, and deriving it then points at the tunnel rather
                    // than at the node.
                    const authorityApiUrl = authorityNode.apiUrl || wsToHttp(authorityNode.connectionUrl);
                  const relayResponse = await fetch(`${authorityApiUrl}/api/rooms/${roomId}/relay`, {
                    method: 'POST',
                    headers: {
                      'Content-Type': 'application/json',
                      Authorization: `Bearer ${roomData.relayToken}`
                    },
                    body: JSON.stringify({ input: disconnectInput })
                  });
                  if (!relayResponse.ok) {
                    console.error(`[DISCONNECT] Failed to relay disconnect input via HTTP: ${relayResponse.status}`);
                  }
                }
              }
            }
          } catch (err) {
            console.error(`[DISCONNECT] Error relaying disconnect input via HTTP:`, err);
            // Fallback to WebSocket relay
            peerManager.broadcastToPeers({
              type: MessageType.RELAY_INPUT,
              payload: { roomId, input: disconnectInput }
            });
          } finally {
            // Also notify authority to update master client list
            peerManager.broadcastToPeers({
              type: MessageType.RELAY_CLIENT_LEAVE,
              payload: { roomId: currentRoomId, clientId }
            });
          }
        })();
      }

      // Remove from active connections, but KEEP in members (they can reconnect)
      roomManager.removeClient(currentRoomId, clientId);
      voiceRelay.forget(currentRoomId, clientId);

      // Tell the room who is still connected.
      //
      // removeClient drops the client from the master list, and nothing said so
      // to anybody: the periodic reconciliation is peers-only by design, so the
      // remaining clients kept a list naming a socket that had closed until the
      // next join happened to push one. Anything a client decides from that list
      // - who to show as present, who publishes snapshots - was working from a
      // membership that could be minutes stale. A disconnect is a change, and
      // changes are what this helper is for.
      if (room && room.isAuthority) {
        syncMasterClientList(currentRoomId, room, peerManager);
      }

      // Same as LEAVE_ROOM: drop their state hashes so a client that dropped off
      // mid-frame stops counting as a voter in the majority-hash consensus.
      roomManager.removeClientFromCollector(currentRoomId, clientId);

      // Update member status to disconnected (they remain a member)
      //
      // Guarded by the connection the member currently names, so a socket that
      // has already been replaced cannot demote the connection that replaced it.
      if (odId) {
        roomManager.setMemberStatus(currentRoomId, odId, 'disconnected', clientId);
      }

      // CRITICAL: Stop tick when room becomes empty to prevent runaway frame counter
      const updatedRoom = roomManager.getRoom(currentRoomId);
      if (updatedRoom && updatedRoom.clients.size === 0) {
        console.log(`[CLEANUP] Room ${currentRoomId} has no active connections, stopping tick`);
        stopRoomTick(currentRoomId);
      }
    }
    console.log(`Client disconnected: ${clientId}`);
  });

  socket.on('error', (error: Error) => {
    console.error(`Client error for ${clientId}:`, error);
  });
}
