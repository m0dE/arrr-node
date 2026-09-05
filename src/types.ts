import type WebSocket from 'ws';

export interface Room {
  id: string;
  snapshot: any;
  snapshotHash: string;
  /**
   * Server-owned metadata about `snapshot`, kept off the client's payload so we
   * never mutate an app's own object or invalidate `snapshotHash`.
   * `snapshotSeqInferred` records that the server guessed `snapshot.seq`
   * because the client did not supply one - a guess that must never be treated
   * as authoritative when selecting catch-up history.
   */
  snapshotFrame?: number;
  snapshotSeqInferred?: boolean;
  /**
   * When the room last became empty, or undefined while anyone is in it. The
   * node uses this to drop rooms nobody returned to; without it they
   * accumulated in memory for the lifetime of the process.
   */
  emptySince?: number;
  /** Recent client-reported desyncs, newest last. Bounded; debugging aid only. */
  desyncReports?: DesyncReport[];
  /**
   * Fingerprint of the simulation code the room's clients are running, taken
   * from the first client to report one. A client that joins with a different
   * fingerprint cannot agree with the others no matter how well the transport
   * behaves, and saying so is far more useful than letting it look like a
   * network fault.
   */
  simVersion?: string;
  snapshotTimestamp: number;
  snapshotSize: number; // Size in bytes
  binarySnapshot?: Buffer;  // Binary snapshot (opaque bytes from engine)
  inputs: NetworkInput[];  // All inputs (joins, leaves, etc.) - opaque to network layer
  clients: Map<string, ClientInfo>;  // Active WebSocket connections only
  members: Map<string, MemberInfo>;  // All room members (connected + disconnected)
  isAuthority: boolean;
  masterClientList?: Map<string, MasterClientEntry>; // Master list of all clients (authority only)
  // Room behavior options
  // If true, membership is removed on disconnect (like session-based connections)
  // If false, membership persists after disconnect (like chat apps)
  removeOnDisconnect: boolean;
  // Track departed client state for late joiner catch-up
  // When a client LEAVES (not just disconnects), we save their last known state
  departedClientState?: Map<string, any>;
  // Clients that have requested resync and are waiting for fresh snapshot
  pendingResyncClients: Set<string>;
  // Backwards compatibility alias (will be removed)
  events?: NetworkInput[];
}

export interface ClientInfo {
  id: string;
  socket: WebSocket;
  joinedAt: number;
  nodeId: string;           // ID of the node this client is connected to
  isMuted: boolean;         // Whether the client is muted
  metadata?: Record<string, any>; // Client metadata from JWT
  // Client index for compact binary encoding (0-255)
  clientIndex?: number;
  // Hash sync tracking
  lastHash?: string;
  lastHashSeq?: number;
  lastHashFrame?: number;
  lastHashTime?: number;
  isSynced?: boolean;
  // Whether INITIAL_STATE has been sent to this client
  initialStateReceived?: boolean;
}

// Master client list entry (used by authority node)
export interface MasterClientEntry {
  clientId: string;
  nodeId: string;
  isMuted: boolean;
  metadata?: Record<string, any>;
  joinedAt: number;
  // Whether this client is still taking part, as opposed to merely still
  // connected. A socket staying open says nothing: a client whose page has been
  // paused, wedged, or throttled to a standstill holds its connection open and
  // sends nothing, and until these existed there was no way to tell the two
  // apart from outside the process. Worse, its avatar keeps moving in the
  // shared world - the last input it sent is still being applied every tick -
  // so watching the simulation cannot detect it either.
  lastInputAt?: number;
  inputsReceived?: number;
  /**
   * How many snapshots this client has published. A room elects one publisher,
   * so more than one client with a non-zero count here means the election is
   * disagreeing with itself - which it did, unseen, for a long time.
   */
  snapshotsPublished?: number;
}
/**
 * MemberInfo tracks room membership independently of active connections.
 * A member can be connected or disconnected - they remain a member until they explicitly leave.
 * This enables the distinction between:
 * - "disconnect" (connection dropped, member persists, can reconnect)
 * - "leave" (permanent exit, removed from membership)
 */
export interface MemberInfo {
  odId: string;                              // user.id from JOIN_ROOM - the member identity
  odMetadata?: any;                          // user metadata from JOIN_ROOM
  clientId?: string;                         // current clientId if connected
  joinedAt: number;                          // when they first joined
  disconnectedAt?: number;                   // when they last disconnected (undefined if connected)
  status: 'connected' | 'disconnected';
  /**
   * The last thing this member's page did was unload itself.
   *
   * Set by a GOING_AWAY message and cleared on reconnect. A member holding it
   * is expired on the short grace rather than the long one: the page is either
   * closing for good or coming straight back, and neither case is the flaky
   * network the long grace exists for.
   */
  unloading?: boolean;
}

// Compact binary input format for TICK messages
export interface CompactInput {
  clientIndex: number;  // 1 byte
  frame: number;        // 2 bytes (delta from tick frame, or lower 16 bits)
  seq: number;          // 2 bytes (lower 16 bits)
  inputType: number;    // 1 byte (0=keys, 1=camera, 2=shoot, etc)
  data: Buffer;         // 1-4 bytes depending on type
}

/**
 * NetworkInput represents any input flowing through the transport layer.
 * All inputs are treated as opaque by the network - the server is a dumb relay
 * that sequences and broadcasts inputs without interpreting their contents.
 *
 * Ordering model:
 * - `frame`: Client-provided timestamp (client's local simulation frame when input was created)
 * - `seq`: Authority-assigned sequence number (for bulk broadcast per tick)
 *
 * Authority sorts inputs by (frame, clientId) before assigning seq. This ensures
 * deterministic ordering across all clients regardless of network latency.
 */
/**
 * A client telling us its state hash disagreed with the room consensus.
 *
 * The client can always see this for itself, but that knowledge used to die in
 * the browser, so a desync in the field left nothing to debug. Kept in memory
 * and readable over HTTP; never fed into the input stream.
 */
export interface DesyncReport {
  clientId: string;
  receivedAt: number;
  frame: number;
  mine: number;
  majority: number;
  player?: string;
  app?: string;
  simFrames?: number;
  inputsApplied?: number;
  restoredFrom?: number | null;
  status?: any;
  /**
   * Why this was recorded, when the server already knows. 'version-mismatch'
   * means the client is running different simulation code and no amount of
   * transport debugging will help it agree.
   */
  reason?: string;
  simVersion?: string;
  expectedSimVersion?: string;
}

export interface NetworkInput {
  id: string;
  clientId: string;
  type: string;           // Transport-level type marker (e.g., 'binary', 'join', 'leave')
  data: any;              // Opaque payload (object for JSON, Buffer for binary)
  seq: number;            // Authority-assigned sequence (for bulk broadcast ordering)
  frame?: number;         // Broadcast frame - set by sendTick(), undefined until then
  clientFrame?: number;   // Client's local frame when input was created (for sorting only)
}

/**
 * Input received from client (before authority assigns seq).
 * Clients send their local frame with each input.
 */
export interface PendingInput {
  id: string;
  clientId: string;
  type: string;
  data: any;
  frame: number;          // Client's local simulation frame when input was created
}

export interface Snapshot {
  data: any;
  hash: string;
  timestamp: number;
  sequenceNumber: number;
}

export enum MessageType {
  // Client → Server (transport layer)
  CREATE_ROOM = 'CREATE_ROOM',
  JOIN_ROOM = 'JOIN_ROOM',
  LEAVE_ROOM = 'LEAVE_ROOM',
  /**
   * "This page is going away" - sent by the SDK as the tab unloads.
   *
   * A closed socket says nothing about why it closed, so a member is held for
   * the full disconnect grace on the chance they are coming back. The browser
   * knows more than the socket does: an unload means the page is either closing
   * or reloading, and both of those resolve within seconds. Saying so lets the
   * node expire a quitter in seconds while still holding a slot for two minutes
   * for someone whose network dropped.
   */
  GOING_AWAY = 'GOING_AWAY',
  SEND_INPUT = 'SEND_INPUT',        // Client sends an input
  SEND_SNAPSHOT = 'SEND_SNAPSHOT',
  GET_CLIENTS = 'GET_CLIENTS',      // Client requests client list
  REQUEST_RESYNC = 'REQUEST_RESYNC', // Client requests state resync (desync recovery)

  // Server → Client (transport layer)
  ROOM_CREATED = 'ROOM_CREATED',
  ROOM_JOINED = 'ROOM_JOINED',
  ROOM_LEFT = 'ROOM_LEFT',
  SNAPSHOT_UPDATE = 'SNAPSHOT_UPDATE',
  INITIAL_STATE = 'INITIAL_STATE',

  // Node-to-node messages
  PEER_IDENTIFY = 'PEER_IDENTIFY',
  PEER_IDENTIFY_ACK = 'PEER_IDENTIFY_ACK',
  SYNC_ROOM_STATE = 'SYNC_ROOM_STATE',
  RELAY_INPUT = 'RELAY_INPUT',      // Relay input to authority node
  // A client's state hash, passed on so agreement is measured across the room
  // rather than within one node. Without it a node judges its clients against
  // only the others that happen to share it, and a client alone on a node is
  // compared against itself - so it can never be found to disagree.
  RELAY_STATE_HASH = 'RELAY_STATE_HASH',
  // A voice packet from a client on another node; see voice-relay.ts.
  RELAY_VOICE = 'RELAY_VOICE',
  BROADCAST_INPUTS = 'BROADCAST_INPUTS',  // Authority broadcasts sequenced inputs
  CLAIM_AUTHORITY = 'CLAIM_AUTHORITY',
  REPLICATE_ROOM = 'REPLICATE_ROOM',
  SYNC_MASTER_CLIENT_LIST = 'SYNC_MASTER_CLIENT_LIST', // Authority → Replicas: sync full master client list
  RELAY_CLIENT_JOIN = 'RELAY_CLIENT_JOIN',  // Replica → Authority: notify of new client
  RELAY_CLIENT_LEAVE = 'RELAY_CLIENT_LEAVE', // Replica → Authority: notify of client leaving
  CLIENT_LIST_UPDATE = 'CLIENT_LIST_UPDATE', // Node → Clients: broadcast updated client list

  // Backwards compatibility aliases - must have original string values for message matching
  SEND_EVENT = 'SEND_EVENT',
  RELAY_EVENT = 'RELAY_EVENT',
  BROADCAST_EVENTS = 'BROADCAST_EVENTS',
  ORDERED_INPUTS = 'ORDERED_INPUTS',

  ERROR = 'ERROR'
}

export interface Message {
  type: MessageType;
  payload: any;
}

export interface PeerNode {
  id: string;
  url: string;
  socket?: WebSocket;
  isConnected: boolean;
}
