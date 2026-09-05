/**
 * Binary Protocol for Node-Client Communication
 *
 * All messages start with a 1-byte type identifier:
 * 0x01 = TICK (frame + optional inputs)
 * 0x02 = INITIAL_STATE (frame + snapshot + inputs)
 * 0x03 = ROOM_JOINED
 * 0x04 = ROOM_CREATED
 * 0x05 = ERROR
 * 0x06 = SNAPSHOT_UPDATE
 * 0x07 = ROOM_LEFT
 * 0x08 = SYNC_HASH (client -> server: roomId + hash + seq + frame)
 * 0x09 = CLIENT_LIST_UPDATE
 * 0x0A = PARTITION_REQUEST (server -> client: request snapshot partition) [deprecated]
 * 0x0B = PARTITION_RESPONSE (client -> server: partition bytes + hash) [deprecated]
 *
 * === Distributed State Sync (0x30+) ===
 * 0x30 = STATE_HASH (client -> server: frame + stateHash)
 * 0x31 = PARTITION_DATA (client -> server: frame + partitionId + data)
 * 0x32 = MAJORITY_HASH (server -> client: frame + majorityHash)
 * 0x33 = RELIABILITY_UPDATE (server -> client: version + scores)
 * 0x34 = DELTA_REQUEST (client -> server: frame)
 * 0x35 = DELTA_RESPONSE (server -> client: frame + delta data)
 */

export const BinaryMessageType = {
  // === Voice (0x40+) ===
  // Off the ordered stream entirely; see voice-relay.ts for the layouts.
  VOICE: 0x40,        // Client -> Node: a talker's frame, or a position beacon
  VOICE_FROM: 0x41,   // Node -> Client: a frame from somebody in earshot
  VOICE_READY: 0x42,  // Node -> Client, once per join: this node relays voice
  TICK: 0x01,
  INITIAL_STATE: 0x02,
  ROOM_JOINED: 0x03,
  ROOM_CREATED: 0x04,
  ERROR: 0x05,
  SNAPSHOT_UPDATE: 0x06,
  ROOM_LEFT: 0x07,
  SYNC_HASH: 0x08,
  CLIENT_LIST_UPDATE: 0x09,
  // Legacy partition-based snapshots (deprecated)
  PARTITION_REQUEST: 0x0A,   // Node -> Client: request partition N of M
  PARTITION_RESPONSE: 0x0B,  // Client -> Node: partition bytes + hash

  // === Distributed State Sync (0x30+) ===
  //
  // Only part of this family is wired up, and it is worth knowing which before
  // reading the rest as a description of what the system does.
  //
  // Live: STATE_HASH, which is how every client votes on the world and how
  // desync detection works at all. PARTITION_DATA is accepted and stored, and
  // then never read - getPartitionData and assignPartitions have no callers.
  //
  // Declared and unused: MAJORITY_HASH, RELIABILITY_UPDATE, DELTA_REQUEST and
  // DELTA_RESPONSE. Nothing encodes or decodes any of them. The majority hash
  // does reach clients, but inside the TICK header rather than as its own
  // message, so 0x32 is a slot rather than a path. See reliability-tracker.ts,
  // which is the same story at module scale.
  //
  // All clients send STATE_HASH after each tick (4-byte xxhash32)
  STATE_HASH: 0x30,          // Client -> Server: [0x30][frame:4][stateHash:4] = 9 bytes
  // Assigned clients send delta partition data
  PARTITION_DATA: 0x31,      // Client -> Server: [0x31][frame:4][partitionId:1][len:2][data:N]
  // Server broadcasts majority hash with TICK for desync detection
  MAJORITY_HASH: 0x32,       // Server -> Client: [0x32][frame:4][majorityHash:4] = 9 bytes
  // Server updates client reliability scores periodically
  RELIABILITY_UPDATE: 0x33,  // Server -> Client: [0x33][version:4][count:1][entries...]
  // Request full delta for late joiners or desync recovery
  DELTA_REQUEST: 0x34,       // Client -> Server: [0x34][frame:4]
  // Full assembled delta from trusted clients
  DELTA_RESPONSE: 0x35       // Server -> Client: [0x35][frame:4][len:4][data:N]
} as const;

/** Largest number of inputs a single TICK's 16-bit count field can describe. */
export const MAX_TICK_INPUTS = 0xffff;

/**
 * Largest single input the tick format can describe.
 *
 * Each input's length is written as a UInt16, so 65535 is a hard property of
 * the encoding rather than a policy choice. Inputs are rejected long before
 * this at the point they arrive; this is the backstop that keeps an encoding
 * limit from being able to kill the process.
 */
const MAX_INPUT_BYTES = 65535;


/**
 * Hash a string ID to a 4-byte identifier (works for any number of clients)
 * Uses simple FNV-1a hash for speed
 * Exported so clients can match hashes to IDs
 */
export function hashClientId(id: string): number {
  let hash = 2166136261; // FNV offset basis
  for (let i = 0; i < id.length; i++) {
    hash ^= id.charCodeAt(i);
    hash = Math.imul(hash, 16777619) >>> 0; // FNV prime, 32-bit integer multiply (must match client!)
  }
  return hash;
}

/**
 * Convert input data to Buffer for binary encoding.
 * - If already Buffer/Uint8Array, use directly (raw bytes from client)
 * - If object, JSON.stringify (for server-generated join/leave events)
 */
function toBuffer(data: any): Buffer {
  if (Buffer.isBuffer(data)) {
    return data;
  } else if (data instanceof Uint8Array) {
    return Buffer.from(data);
  } else if (data && data.type === 'Buffer' && Array.isArray(data.data)) {
    // Reconstruct Buffer from JSON-serialized form (happens after peer relay)
    return Buffer.from(data.data);
  } else {
    return Buffer.from(JSON.stringify(data || {}), 'utf8');
  }
}


/**
 * Encode a TICK message (server -> client)
 *
 * Format: [0x01] [frame:4] [snapshotFrame:4] [majorityHash:4] [hashLen:1] [snapshotHash:hashLen] [count:2] [inputs...]
 * Each input: [clientHash:4] [seq:4] [dataLen:2] [data:dataLen]
 *
 * count is 16-bit. It used to be a single byte, which silently truncated any
 * tick carrying more than 255 inputs: the buffer held them all, the count said
 * (n mod 256), and every client stopped decoding partway through with no error
 * raised anywhere. INITIAL_STATE has always used a 16-bit count, so a late
 * joiner replaying history saw inputs the live clients had dropped and desynced
 * from them permanently. See tests/test-tick-input-count.ts.
 *
 * majorityHash is the consensus hash computed from clients' STATE_HASH submissions.
 * Clients compare their local state hash against majorityHash to detect desync.
 * majorityHash=0 means no majority established yet (not enough clients or tie).
 *
 * snapshotHash is the hash of the server's stored snapshot (legacy, for backwards compat).
 * snapshotFrame is the frame when that snapshot was taken.
 * hashLen=0 means no snapshot available yet.
 *
 * Server is data-agnostic: stores raw bytes from clients, sends raw bytes to clients.
 */
export function encodeTick(frame: number, inputs: any[], snapshotHash?: string, snapshotFrame?: number, majorityHash?: number): Buffer {
  const hashBuffer = snapshotHash ? Buffer.from(snapshotHash, 'utf8') : Buffer.alloc(0);
  const hashLen = hashBuffer.length;
  const snapFrame = snapshotFrame || 0;
  const majHash = majorityHash ?? 0;

  if (inputs.length === 0) {
    // Empty tick: type + frame + snapshotFrame + majorityHash + hashLen + hash (14 + hashLen bytes)
    const message = Buffer.alloc(14 + hashLen);
    message[0] = BinaryMessageType.TICK;
    message.writeUInt32LE(frame, 1);
    message.writeUInt32LE(snapFrame, 5);
    message.writeUInt32LE(majHash >>> 0, 9);
    message[13] = hashLen;
    if (hashLen > 0) hashBuffer.copy(message, 14);
    return message;
  }

  // A tick can only describe as many inputs as the count field can express.
  // Going over is a real possibility with an unthrottled client, so drop the
  // overflow loudly instead of writing a buffer the decoder cannot read.
  let batch = inputs;
  if (inputs.length > MAX_TICK_INPUTS) {
    console.error(
      `[TICK] frame=${frame} batched ${inputs.length} inputs, over the per-tick ceiling of ${MAX_TICK_INPUTS}; dropping ${inputs.length - MAX_TICK_INPUTS}`
    );
    batch = inputs.slice(0, MAX_TICK_INPUTS);
  }

  // Encode each input - pass through raw bytes or JSON encode objects (for join/leave events)
  const encodedInputs: { clientHash: number; seq: number; data: Buffer }[] = [];
  let totalDataSize = 16 + hashLen; // header: type(1) + frame(4) + snapshotFrame(4) + majorityHash(4) + hashLen(1) + hash + count(2)

  for (const inp of batch) {
    const seq = inp.seq || 0;
    const dataBuffer = toBuffer(inp.data);

    // An input's length is written as a UInt16, so one that does not fit cannot
    // be described by this format at all. Writing it anyway throws, and this
    // runs inside the room's tick timer - so the exception is not caught by
    // anyone, and the entire node process dies, taking every room and every
    // player on it with it. One client sending one 400KB message did exactly
    // that. Dropping the input costs that client its message; throwing costs
    // everybody else their session.
    if (dataBuffer.length > MAX_INPUT_BYTES) {
      console.error(
        `[TICK] frame=${frame} dropping a ${dataBuffer.length}-byte input from ${inp.clientId || 'unknown'}: `
        + `the wire format cannot describe anything over ${MAX_INPUT_BYTES} bytes`
      );
      continue;
    }

    // Use clientId for hash (network is agnostic - doesn't know about "players")
    const clientId = inp.clientId || '';
    const clientHash = hashClientId(clientId);

    encodedInputs.push({ clientHash, seq, data: dataBuffer });
    totalDataSize += 4 + 4 + 2 + dataBuffer.length; // clientHash(4) + seq(4) + dataLen(2) + data
  }

  // Build the message
  const message = Buffer.alloc(totalDataSize);
  let offset = 0;
  message[offset++] = BinaryMessageType.TICK;
  message.writeUInt32LE(frame, offset); offset += 4;
  message.writeUInt32LE(snapFrame, offset); offset += 4;
  message.writeUInt32LE(majHash >>> 0, offset); offset += 4;
  message[offset++] = hashLen;
  if (hashLen > 0) { hashBuffer.copy(message, offset); offset += hashLen; }
  message.writeUInt16LE(encodedInputs.length, offset); offset += 2;

  for (const enc of encodedInputs) {
    message.writeUInt32LE(enc.clientHash, offset); offset += 4;
    message.writeUInt32LE(enc.seq, offset); offset += 4; // UInt32 to support >65535 inputs
    message.writeUInt16LE(enc.data.length, offset); offset += 2;
    enc.data.copy(message, offset);
    offset += enc.data.length;
  }

  return message;
}

/**
 * Legacy JSON-based encodeTick for backwards compatibility
 * Use encodeTick() for compact binary encoding
 */
export function encodeTickJson(frame: number, inputs: any[]): Buffer {
  if (inputs.length === 0) {
    const message = Buffer.alloc(5);
    message[0] = BinaryMessageType.TICK;
    message.writeUInt32LE(frame, 1);
    return message;
  }

  const compactInputs = inputs.map(inp => ({
    seq: inp.seq,
    data: inp.data
  }));

  const inputsJson = JSON.stringify(compactInputs);
  const inputsBuffer = Buffer.from(inputsJson, 'utf8');
  const message = Buffer.alloc(7 + inputsBuffer.length);
  message[0] = BinaryMessageType.TICK;
  message.writeUInt32LE(frame, 1);
  message.writeUInt16LE(inputs.length, 5);
  inputsBuffer.copy(message, 7);
  return message;
}

/**
 * Encode INITIAL_STATE message (server -> client)
 * Format: [0x02] [frame:4] [roomIdLen:2] [roomId] [snapshotLen:4] [snapshotData]
 *         [clientIdCount:2] [clientIds: hash:4, len:2, id...]
 *         [inputCount:2] [inputs in binary format same as TICK]
 * Each input: [clientHash:4] [seq:4] [frame:4] [dataLen:2] [data:dataLen]
 *
 * Uses binary format - snapshot is opaque bytes from engine.
 * clientIds section contains hash->clientId mappings so SDK can decode inputs.
 */
/**
 * Encode INITIAL_STATE message (server -> client)
 *
 * Format: [0x02][frame:4][roomIdLen:2][roomId][snapshotLen:4][snapshot][inputCount:2][inputs...]
 * Each input: [clientHash:4][seq:4][frame:4][dataLen:2][data]
 *
 * Inputs use clientHash (same as TICK) for bandwidth efficiency.
 * The snapshot contains clientIdMap.toNum with full clientId strings.
 * SDK decodes snapshot, extracts clientIds, then resolves input hashes.
 */
export function encodeInitialState(roomId: string, frame: number, snapshot: any, snapshotHash: string, inputs: any[], binarySnapshot?: Buffer): Buffer {
  const roomIdBuffer = Buffer.from(roomId, 'utf8');

  // Use binary snapshot if provided, otherwise JSON-encode the snapshot object
  let snapshotBuffer: Buffer;
  if (binarySnapshot && binarySnapshot.length > 0) {
    snapshotBuffer = binarySnapshot;
  } else if (snapshot && Object.keys(snapshot).length > 0) {
    const snapshotJson = JSON.stringify({ snapshot, snapshotHash });
    snapshotBuffer = Buffer.from(snapshotJson, 'utf8');
  } else {
    snapshotBuffer = Buffer.alloc(0);
  }

  // Encode inputs with clientHash (same format as TICK)
  // Same 16-bit count ceiling as encodeTick - clamp loudly rather than letting
  // the count wrap and hand the client a message it will mis-decode.
  let batch = inputs;
  if (inputs.length > MAX_TICK_INPUTS) {
    console.error(
      `[INITIAL_STATE] room=${roomId} has ${inputs.length} catch-up inputs, over the ceiling of ${MAX_TICK_INPUTS}; dropping the oldest ${inputs.length - MAX_TICK_INPUTS}`
    );
    // Keep the NEWEST, since a late joiner replays forward from the snapshot.
    batch = inputs.slice(inputs.length - MAX_TICK_INPUTS);
  }

  const encodedInputs: { clientHash: number; seq: number; frame: number; data: Buffer }[] = [];
  let inputsSize = 2; // inputCount:2

  for (const inp of batch) {
    const dataBuffer = toBuffer(inp.data);

    // The same UInt16 length field as a tick carries, and the same consequence
    // for exceeding it. This encoder was overlooked when that was fixed in the
    // tick path, which is the ordinary way a fix half-lands: the crash was
    // reproduced through one encoder, so only that one got looked at. A late
    // joiner is handed this history, so an input nobody can describe would have
    // failed their catch-up rather than the room's tick - quieter, and just as
    // broken.
    if (dataBuffer.length > MAX_INPUT_BYTES) {
      console.error(
        `[INITIAL_STATE] dropping a ${dataBuffer.length}-byte input from ${inp.clientId || 'unknown'}: `
        + `over the ${MAX_INPUT_BYTES}-byte field`
      );
      continue;
    }

    const clientHash = hashClientId(inp.clientId || '');
    encodedInputs.push({ clientHash, seq: inp.seq || 0, frame: inp.frame || 0, data: dataBuffer });
    inputsSize += 4 + 4 + 4 + 2 + dataBuffer.length; // clientHash + seq + frame + dataLen + data
  }

  // Total: type + frame + roomIdLen + roomId + snapshotLen + snapshot + inputs
  const totalLen = 1 + 4 + 2 + roomIdBuffer.length + 4 + snapshotBuffer.length + inputsSize;
  const message = Buffer.alloc(totalLen);

  let offset = 0;
  message[offset++] = BinaryMessageType.INITIAL_STATE;
  message.writeUInt32LE(frame, offset); offset += 4;
  message.writeUInt16LE(roomIdBuffer.length, offset); offset += 2;
  roomIdBuffer.copy(message, offset); offset += roomIdBuffer.length;
  message.writeUInt32LE(snapshotBuffer.length, offset); offset += 4;
  snapshotBuffer.copy(message, offset); offset += snapshotBuffer.length;

  // Write inputs with clientHash (same format as TICK)
  message.writeUInt16LE(encodedInputs.length, offset); offset += 2;
  for (const enc of encodedInputs) {
    message.writeUInt32LE(enc.clientHash, offset); offset += 4;
    message.writeUInt32LE(enc.seq, offset); offset += 4;
    message.writeUInt32LE(enc.frame, offset); offset += 4;
    message.writeUInt16LE(enc.data.length, offset); offset += 2;
    enc.data.copy(message, offset); offset += enc.data.length;
  }

  return message;
}

/**
 * Encode ROOM_JOINED message (server -> client)
 * Format: [0x03] [2 bytes roomId len] [roomId] [2 bytes clientId len] [clientId]
 */
export function encodeRoomJoined(roomId: string, clientId: string): Buffer {
  const roomIdBuffer = Buffer.from(roomId, 'utf8');
  const clientIdBuffer = Buffer.from(clientId, 'utf8');
  const message = Buffer.alloc(1 + 2 + roomIdBuffer.length + 2 + clientIdBuffer.length);
  let offset = 0;
  message[offset++] = BinaryMessageType.ROOM_JOINED;
  message.writeUInt16LE(roomIdBuffer.length, offset); offset += 2;
  roomIdBuffer.copy(message, offset); offset += roomIdBuffer.length;
  message.writeUInt16LE(clientIdBuffer.length, offset); offset += 2;
  clientIdBuffer.copy(message, offset);
  return message;
}

/**
 * Encode ROOM_CREATED message (server -> client)
 * Format: [0x04] [2 bytes roomId len] [roomId] [2 bytes clientId len] [clientId] [4 bytes snapshot len] [snapshot JSON]
 */
export function encodeRoomCreated(roomId: string, clientId: string, snapshot: any, snapshotHash: string): Buffer {
  const roomIdBuffer = Buffer.from(roomId, 'utf8');
  const clientIdBuffer = Buffer.from(clientId, 'utf8');
  const snapshotJson = JSON.stringify({ snapshot, snapshotHash });
  const snapshotBuffer = Buffer.from(snapshotJson, 'utf8');

  const message = Buffer.alloc(1 + 2 + roomIdBuffer.length + 2 + clientIdBuffer.length + 4 + snapshotBuffer.length);
  let offset = 0;
  message[offset++] = BinaryMessageType.ROOM_CREATED;
  message.writeUInt16LE(roomIdBuffer.length, offset); offset += 2;
  roomIdBuffer.copy(message, offset); offset += roomIdBuffer.length;
  message.writeUInt16LE(clientIdBuffer.length, offset); offset += 2;
  clientIdBuffer.copy(message, offset); offset += clientIdBuffer.length;
  message.writeUInt32LE(snapshotBuffer.length, offset); offset += 4;
  snapshotBuffer.copy(message, offset);

  return message;
}

/**
 * Encode ERROR message (server -> client)
 * Format: [0x05] [2 bytes message len] [message]
 */
export function encodeError(errorMessage: string): Buffer {
  const msgBuffer = Buffer.from(errorMessage, 'utf8');
  const message = Buffer.alloc(1 + 2 + msgBuffer.length);
  message[0] = BinaryMessageType.ERROR;
  message.writeUInt16LE(msgBuffer.length, 1);
  msgBuffer.copy(message, 3);
  return message;
}

/**
 * Encode SNAPSHOT_UPDATE message (server -> client)
 * Format: [0x06] [2 bytes roomId len] [roomId] [4 bytes snapshot len] [snapshot JSON]
 */
export function encodeSnapshotUpdate(roomId: string, snapshot: any, snapshotHash: string): Buffer {
  const roomIdBuffer = Buffer.from(roomId, 'utf8');
  const snapshotJson = JSON.stringify({ snapshot, snapshotHash });
  const snapshotBuffer = Buffer.from(snapshotJson, 'utf8');

  const message = Buffer.alloc(1 + 2 + roomIdBuffer.length + 4 + snapshotBuffer.length);
  let offset = 0;
  message[offset++] = BinaryMessageType.SNAPSHOT_UPDATE;
  message.writeUInt16LE(roomIdBuffer.length, offset); offset += 2;
  roomIdBuffer.copy(message, offset); offset += roomIdBuffer.length;
  message.writeUInt32LE(snapshotBuffer.length, offset); offset += 4;
  snapshotBuffer.copy(message, offset);

  return message;
}

/**
 * Encode ROOM_LEFT message (server -> client)
 * Format: [0x07] [2 bytes roomId len] [roomId]
 */
export function encodeRoomLeft(roomId: string): Buffer {
  const roomIdBuffer = Buffer.from(roomId, 'utf8');
  const message = Buffer.alloc(1 + 2 + roomIdBuffer.length);
  message[0] = BinaryMessageType.ROOM_LEFT;
  message.writeUInt16LE(roomIdBuffer.length, 1);
  roomIdBuffer.copy(message, 3);
  return message;
}

/**
 * Encode CLIENT_LIST_UPDATE message (server -> client)
 * Format: [0x09] [2 bytes roomId len] [roomId] [4 bytes clients JSON len] [clients JSON]
 */
export function encodeClientListUpdate(roomId: string, clients: any[]): Buffer {
  const roomIdBuffer = Buffer.from(roomId, 'utf8');
  const clientsBuffer = Buffer.from(JSON.stringify(clients), 'utf8');

  const message = Buffer.alloc(1 + 2 + roomIdBuffer.length + 4 + clientsBuffer.length);
  let offset = 0;
  message[offset++] = BinaryMessageType.CLIENT_LIST_UPDATE;
  message.writeUInt16LE(roomIdBuffer.length, offset); offset += 2;
  roomIdBuffer.copy(message, offset); offset += roomIdBuffer.length;
  message.writeUInt32LE(clientsBuffer.length, offset); offset += 4;
  clientsBuffer.copy(message, offset);

  return message;
}

// Type definitions for decoded messages
// IMPORTANT: The network layer is data-agnostic. Payloads are opaque bytes.
// The server only decodes transport framing (type, frame, seq, clientHash, lengths).
// Payload data remains as raw Buffer - interpretation is the client/engine's responsibility.

export interface DecodedTickInput {
  clientHash: number;
  seq: number;
  data: Buffer;  // Opaque bytes - server does NOT interpret
}

export interface DecodedTick {
  type: 'TICK';
  frame: number;
  snapshotFrame?: number;  // Frame when server's snapshot was taken
  snapshotHash?: string;   // Hash of server's snapshot for drift detection
  majorityHash?: number;   // Consensus hash from clients' STATE_HASH submissions
  inputs: DecodedTickInput[];  // Raw bytes per input
}

export interface DecodedInitialStateInput {
  clientHash: number;
  seq: number;
  data: Buffer;  // Opaque bytes - server does NOT interpret
}

export interface DecodedInitialState {
  type: 'INITIAL_STATE';
  roomId: string;
  frame: number;
  snapshotData: Buffer;  // Opaque snapshot bytes - server stores/forwards without interpretation
  inputs: DecodedInitialStateInput[];  // Raw bytes per input
}

export interface DecodedRoomJoined {
  type: 'ROOM_JOINED';
  roomId: string;
  clientId: string;
}

export interface DecodedRoomCreated {
  type: 'ROOM_CREATED';
  roomId: string;
  clientId: string;
  snapshotData: Buffer;  // Opaque snapshot bytes - server stores/forwards without interpretation
}

export interface DecodedError {
  type: 'ERROR';
  message: string;
}

export interface DecodedSnapshotUpdate {
  type: 'SNAPSHOT_UPDATE';
  roomId: string;
  snapshotData: Buffer;  // Opaque snapshot bytes - server stores/forwards without interpretation
}

export interface DecodedRoomLeft {
  type: 'ROOM_LEFT';
  roomId: string;
}

export interface DecodedSyncHash {
  type: 'SYNC_HASH';
  roomId: string;
  hash: string;
  seq: number;
  frame: number;
}

export interface DecodedClientListUpdate {
  type: 'CLIENT_LIST_UPDATE';
  roomId: string;
  clientsData: Buffer;  // Opaque client list bytes - server stores/forwards without interpretation
}

export type DecodedMessage =
  | DecodedTick
  | DecodedInitialState
  | DecodedRoomJoined
  | DecodedRoomCreated
  | DecodedError
  | DecodedSnapshotUpdate
  | DecodedRoomLeft
  | DecodedSyncHash
  | DecodedClientListUpdate
  | DecodedStateHash
  | DecodedPartitionData
  | DecodedMajorityHash
  | DecodedReliabilityUpdate
  | DecodedDeltaRequest
  | DecodedDeltaResponse;

/**
 * Decode a binary message from server
 * Works with both Buffer (Node.js) and ArrayBuffer (browser)
 *
 * IMPORTANT: This function is DATA-AGNOSTIC. It only decodes transport framing
 * (message type, room ID, frame, seq, clientHash, lengths) but NOT payload content.
 * Payloads are returned as raw Buffer for the application layer to interpret.
 */
/**
 * Decode a binary message, or return null if it is not one.
 *
 * This reads bytes a client put on a socket, so every length in it is a claim
 * rather than a fact, and it must refuse rather than throw. It did throw: a
 * single byte - just the TICK type, nothing after it - reads a frame number
 * from offset 1 and walks off the end of the buffer. Five of six deliberately
 * malformed messages threw, including that one, and it is a byte any client can
 * send.
 *
 * The socket's message handler catches, so today this costs the sender rather
 * than the node. That is luck rather than design: the same shape one layer over
 * killed the process when it happened inside a timer. A parser over untrusted
 * input should return null and say so, which is what the browser SDK's decoder
 * already does - it wraps its switch exactly like this.
 */
export function decodeMessage(data: Buffer | ArrayBuffer): DecodedMessage | null {
  const buffer = data instanceof ArrayBuffer ? Buffer.from(data) : data;
  if (buffer.length === 0) return null;

  const type = buffer[0];

  try {
    return decodeMessageUnchecked(buffer, type);
  } catch (err) {
    // Truncated, or lying about a length. Either way it is not decodable, and
    // the sender learns nothing from us beyond the message being ignored.
    console.error(`[DECODE] Refusing a malformed type-${type} message of ${buffer.length} bytes: ${(err as Error).message}`);
    return null;
  }
}

function decodeMessageUnchecked(buffer: Buffer, type: number): DecodedMessage | null {
  switch (type) {
    case BinaryMessageType.TICK: {
      // Cheap and by far the likeliest truncation, so it is refused directly
      // rather than by way of an exception.
      if (buffer.length < 5) return null;
      const frame = buffer.readUInt32LE(1);
      const inputs: DecodedTickInput[] = [];
      let snapshotFrame: number | undefined;
      let snapshotHash: string | undefined;
      let majorityHash: number | undefined;

      if (buffer.length >= 14) {
        // Must mirror encodeTick exactly:
        // [type:1][frame:4][snapshotFrame:4][majorityHash:4][hashLen:1][hash:hashLen][count:2][inputs...]
        // Each input: [clientHash:4][seq:4][dataLen:2][data:dataLen]
        snapshotFrame = buffer.readUInt32LE(5);
        majorityHash = buffer.readUInt32LE(9);
        const hashLen = buffer[13];
        let offset = 14;

        if (hashLen > 0 && offset + hashLen <= buffer.length) {
          snapshotHash = buffer.slice(offset, offset + hashLen).toString('utf8');
          offset += hashLen;
        }

        if (offset + 2 <= buffer.length) {
          const inputCount = buffer.readUInt16LE(offset); offset += 2;

          for (let i = 0; i < inputCount && offset < buffer.length; i++) {
            const clientHash = buffer.readUInt32LE(offset); offset += 4;
            const seq = buffer.readUInt32LE(offset); offset += 4;
            const dataLen = buffer.readUInt16LE(offset); offset += 2;

            if (offset + dataLen > buffer.length) break;

            // DATA-AGNOSTIC: Return raw bytes - do NOT parse or interpret
            const data = Buffer.from(buffer.slice(offset, offset + dataLen));
            offset += dataLen;

            inputs.push({ clientHash, seq, data });
          }
        }
      }
      return { type: 'TICK', frame, snapshotFrame, snapshotHash, majorityHash, inputs };
    }

    case BinaryMessageType.INITIAL_STATE: {
      let offset = 1;
      const frame = buffer.readUInt32LE(offset); offset += 4;
      const roomIdLen = buffer.readUInt16LE(offset); offset += 2;
      const roomId = buffer.slice(offset, offset + roomIdLen).toString('utf8'); offset += roomIdLen;
      const snapshotLen = buffer.readUInt32LE(offset); offset += 4;

      // DATA-AGNOSTIC: Return snapshot as raw bytes - do NOT parse
      const snapshotData = Buffer.from(buffer.slice(offset, offset + snapshotLen));
      offset += snapshotLen;

      // Decode inputs in binary format (same as TICK)
      const inputCount = buffer.readUInt16LE(offset); offset += 2;
      const inputs: DecodedInitialStateInput[] = [];

      for (let i = 0; i < inputCount && offset < buffer.length; i++) {
        const clientHash = buffer.readUInt32LE(offset); offset += 4;
        const seq = buffer.readUInt32LE(offset); offset += 4;
        const dataLen = buffer.readUInt16LE(offset); offset += 2;

        if (offset + dataLen > buffer.length) break;

        // DATA-AGNOSTIC: Return raw bytes - do NOT parse or interpret
        const data = Buffer.from(buffer.slice(offset, offset + dataLen));
        offset += dataLen;

        inputs.push({ clientHash, seq, data });
      }

      return { type: 'INITIAL_STATE', roomId, frame, snapshotData, inputs };
    }

    case BinaryMessageType.ROOM_JOINED: {
      let offset = 1;
      const roomIdLen = buffer.readUInt16LE(offset); offset += 2;
      const roomId = buffer.slice(offset, offset + roomIdLen).toString('utf8'); offset += roomIdLen;
      const clientIdLen = buffer.readUInt16LE(offset); offset += 2;
      const clientId = buffer.slice(offset, offset + clientIdLen).toString('utf8');
      return { type: 'ROOM_JOINED', roomId, clientId };
    }

    case BinaryMessageType.ROOM_CREATED: {
      let offset = 1;
      const roomIdLen = buffer.readUInt16LE(offset); offset += 2;
      const roomId = buffer.slice(offset, offset + roomIdLen).toString('utf8'); offset += roomIdLen;
      const clientIdLen = buffer.readUInt16LE(offset); offset += 2;
      const clientId = buffer.slice(offset, offset + clientIdLen).toString('utf8'); offset += clientIdLen;
      const snapshotLen = buffer.readUInt32LE(offset); offset += 4;

      // DATA-AGNOSTIC: Return snapshot as raw bytes - do NOT parse
      const snapshotData = Buffer.from(buffer.slice(offset, offset + snapshotLen));

      return { type: 'ROOM_CREATED', roomId, clientId, snapshotData };
    }

    case BinaryMessageType.ERROR: {
      const msgLen = buffer.readUInt16LE(1);
      const message = buffer.slice(3, 3 + msgLen).toString('utf8');
      return { type: 'ERROR', message };
    }

    case BinaryMessageType.SNAPSHOT_UPDATE: {
      let offset = 1;
      const roomIdLen = buffer.readUInt16LE(offset); offset += 2;
      const roomId = buffer.slice(offset, offset + roomIdLen).toString('utf8'); offset += roomIdLen;
      const snapshotLen = buffer.readUInt32LE(offset); offset += 4;

      // DATA-AGNOSTIC: Return snapshot as raw bytes - do NOT parse
      const snapshotData = Buffer.from(buffer.slice(offset, offset + snapshotLen));

      return { type: 'SNAPSHOT_UPDATE', roomId, snapshotData };
    }

    case BinaryMessageType.ROOM_LEFT: {
      const roomIdLen = buffer.readUInt16LE(1);
      const roomId = buffer.slice(3, 3 + roomIdLen).toString('utf8');
      return { type: 'ROOM_LEFT', roomId };
    }

    case BinaryMessageType.SYNC_HASH: {
      let offset = 1;
      const roomIdLen = buffer.readUInt16LE(offset); offset += 2;
      const roomId = buffer.slice(offset, offset + roomIdLen).toString('utf8'); offset += roomIdLen;
      const hashLen = buffer.readUInt16LE(offset); offset += 2;
      const hash = buffer.slice(offset, offset + hashLen).toString('utf8'); offset += hashLen;
      const seq = buffer.readUInt32LE(offset); offset += 4;
      const frame = buffer.readUInt32LE(offset);
      return { type: 'SYNC_HASH', roomId, hash, seq, frame };
    }

    case BinaryMessageType.CLIENT_LIST_UPDATE: {
      let offset = 1;
      const roomIdLen = buffer.readUInt16LE(offset); offset += 2;
      const roomId = buffer.slice(offset, offset + roomIdLen).toString('utf8'); offset += roomIdLen;
      const clientsLen = buffer.readUInt32LE(offset); offset += 4;

      // DATA-AGNOSTIC: Return client list as raw bytes - do NOT parse
      const clientsData = Buffer.from(buffer.slice(offset, offset + clientsLen));

      return { type: 'CLIENT_LIST_UPDATE', roomId, clientsData };
    }

    case BinaryMessageType.STATE_HASH: {
      // Format: [0x30][frame:4][stateHash:4] = 9 bytes
      if (buffer.length < 9) return null;
      const frame = buffer.readUInt32LE(1);
      const stateHash = buffer.readUInt32LE(5);
      return { type: 'STATE_HASH', frame, stateHash } as DecodedStateHash;
    }

    case BinaryMessageType.PARTITION_DATA: {
      // Format: [0x31][frame:4][partitionId:1][len:2][data:N]
      if (buffer.length < 8) return null;
      const frame = buffer.readUInt32LE(1);
      const partitionId = buffer[5];
      const dataLen = buffer.readUInt16LE(6);
      if (buffer.length < 8 + dataLen) return null;
      const data = Buffer.from(buffer.slice(8, 8 + dataLen));
      return { type: 'PARTITION_DATA', frame, partitionId, data } as DecodedPartitionData;
    }

    case BinaryMessageType.MAJORITY_HASH: {
      // Format: [0x32][frame:4][majorityHash:4] = 9 bytes
      if (buffer.length < 9) return null;
      const frame = buffer.readUInt32LE(1);
      const majorityHash = buffer.readUInt32LE(5);
      return { type: 'MAJORITY_HASH', frame, majorityHash } as DecodedMajorityHash;
    }

    case BinaryMessageType.RELIABILITY_UPDATE: {
      // Format: [0x33][version:4][count:1][entries...]
      // Each entry: [clientIdLen:1][clientId:N][score:1]
      if (buffer.length < 6) return null;
      const version = buffer.readUInt32LE(1);
      const count = buffer[5];
      const scores: Record<string, number> = {};
      let offset = 6;
      for (let i = 0; i < count && offset < buffer.length; i++) {
        const clientIdLen = buffer[offset++];
        if (offset + clientIdLen + 1 > buffer.length) break;
        const clientId = buffer.slice(offset, offset + clientIdLen).toString('utf8');
        offset += clientIdLen;
        const score = buffer[offset++];
        scores[clientId] = score;
      }
      return { type: 'RELIABILITY_UPDATE', version, scores } as DecodedReliabilityUpdate;
    }

    case BinaryMessageType.DELTA_REQUEST: {
      // Format: [0x34][frame:4]
      if (buffer.length < 5) return null;
      const frame = buffer.readUInt32LE(1);
      return { type: 'DELTA_REQUEST', frame } as DecodedDeltaRequest;
    }

    case BinaryMessageType.DELTA_RESPONSE: {
      // Format: [0x35][frame:4][len:4][data:N]
      if (buffer.length < 9) return null;
      const frame = buffer.readUInt32LE(1);
      const dataLen = buffer.readUInt32LE(5);
      if (buffer.length < 9 + dataLen) return null;
      const data = Buffer.from(buffer.slice(9, 9 + dataLen));
      return { type: 'DELTA_RESPONSE', frame, data } as DecodedDeltaResponse;
    }

    default:
      return null;
  }
}

// ============================================
// Distributed State Sync Encoders
// ============================================

/**
 * Encode STATE_HASH message (client -> server)
 * Format: [0x30][frame:4][stateHash:4] = 9 bytes
 */
export function encodeStateHash(frame: number, stateHash: number): Buffer {
  const buffer = Buffer.alloc(9);
  buffer[0] = BinaryMessageType.STATE_HASH;
  buffer.writeUInt32LE(frame, 1);
  buffer.writeUInt32LE(stateHash >>> 0, 5);
  return buffer;
}

/**
 * Encode PARTITION_DATA message (client -> server)
 * Format: [0x31][frame:4][partitionId:1][len:2][data:N]
 */
export function encodePartitionData(frame: number, partitionId: number, data: Buffer): Buffer {
  const buffer = Buffer.alloc(8 + data.length);
  buffer[0] = BinaryMessageType.PARTITION_DATA;
  buffer.writeUInt32LE(frame, 1);
  buffer[5] = partitionId;
  buffer.writeUInt16LE(data.length, 6);
  data.copy(buffer, 8);
  return buffer;
}

/**
 * Encode MAJORITY_HASH message (server -> client)
 * Format: [0x32][frame:4][majorityHash:4] = 9 bytes
 */
export function encodeMajorityHash(frame: number, majorityHash: number): Buffer {
  const buffer = Buffer.alloc(9);
  buffer[0] = BinaryMessageType.MAJORITY_HASH;
  buffer.writeUInt32LE(frame, 1);
  buffer.writeUInt32LE(majorityHash >>> 0, 5);
  return buffer;
}

/**
 * Encode RELIABILITY_UPDATE message (server -> client)
 * Format: [0x33][version:4][count:1][entries...]
 * Each entry: [clientIdLen:1][clientId:N][score:1]
 */
export function encodeReliabilityUpdate(version: number, scores: Record<string, number>): Buffer {
  const entries = Object.entries(scores);
  let size = 6; // type(1) + version(4) + count(1)
  for (const [clientId] of entries) {
    size += 1 + Buffer.byteLength(clientId, 'utf8') + 1; // len + clientId + score
  }

  const buffer = Buffer.alloc(size);
  let offset = 0;
  buffer[offset++] = BinaryMessageType.RELIABILITY_UPDATE;
  buffer.writeUInt32LE(version, offset); offset += 4;
  buffer[offset++] = entries.length;

  for (const [clientId, score] of entries) {
    const clientIdBuffer = Buffer.from(clientId, 'utf8');
    buffer[offset++] = clientIdBuffer.length;
    clientIdBuffer.copy(buffer, offset); offset += clientIdBuffer.length;
    buffer[offset++] = Math.max(0, Math.min(100, Math.round(score))); // Clamp to 0-100
  }

  return buffer;
}

/**
 * Encode DELTA_REQUEST message (client -> server)
 * Format: [0x34][frame:4]
 */
export function encodeDeltaRequest(frame: number): Buffer {
  const buffer = Buffer.alloc(5);
  buffer[0] = BinaryMessageType.DELTA_REQUEST;
  buffer.writeUInt32LE(frame, 1);
  return buffer;
}

/**
 * Encode DELTA_RESPONSE message (server -> client)
 * Format: [0x35][frame:4][len:4][data:N]
 */
export function encodeDeltaResponse(frame: number, data: Buffer): Buffer {
  const buffer = Buffer.alloc(9 + data.length);
  buffer[0] = BinaryMessageType.DELTA_RESPONSE;
  buffer.writeUInt32LE(frame, 1);
  buffer.writeUInt32LE(data.length, 5);
  data.copy(buffer, 9);
  return buffer;
}

// ============================================
// Distributed State Sync Decoded Types
// ============================================

export interface DecodedStateHash {
  type: 'STATE_HASH';
  frame: number;
  stateHash: number;
}

export interface DecodedPartitionData {
  type: 'PARTITION_DATA';
  frame: number;
  partitionId: number;
  data: Buffer;
}

export interface DecodedMajorityHash {
  type: 'MAJORITY_HASH';
  frame: number;
  majorityHash: number;
}

export interface DecodedReliabilityUpdate {
  type: 'RELIABILITY_UPDATE';
  version: number;
  scores: Record<string, number>;
}

export interface DecodedDeltaRequest {
  type: 'DELTA_REQUEST';
  frame: number;
}

export interface DecodedDeltaResponse {
  type: 'DELTA_RESPONSE';
  frame: number;
  data: Buffer;
}
