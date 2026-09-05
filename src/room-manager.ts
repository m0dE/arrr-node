import crypto from 'crypto';
import { Room, NetworkInput, Snapshot, ClientInfo, MemberInfo, DesyncReport } from './types';
import type WebSocket from 'ws';
import { PartitionCollector } from './partition-collector';
import { pendingPeerInputCount, tickStateCount, cleanupRoomTick, dropOrphanedTickStates, getCurrentFrame } from './input-batcher';

/**
 * How far from the room's own frame a client's vote may claim to be.
 *
 * Ahead is tight: a client simulates frames the server has already broadcast,
 * so it is never meaningfully in front, and this is the direction the attack
 * uses. Behind is loose, because a client catching up replays and votes on
 * every frame it passes through.
 */
const HASH_VOTE_AHEAD = 60;    // 3s at 20Hz
const HASH_VOTE_BEHIND = 600;  // 30s at 20Hz

// Max inputs to keep per room (prevents unbounded memory growth)
// At 30Hz tick rate: 10000 inputs = ~5.5 minutes of history
const MAX_INPUTS_PER_ROOM = 10000;

// Desync reports retained per room for debugging.
const MAX_DESYNC_REPORTS = 50;

// How often to run maintenance tasks (ms)
const MAINTENANCE_INTERVAL = 5000;

/**
 * How long a room with nobody in it is kept before the node drops it.
 *
 * Rooms were never removed here at all. Maintenance skipped any room with no
 * clients, and the only path to deleteRoom was an HTTP endpoint that central
 * calls during its own cleanup - which needs a database, so in any deployment
 * without one (play mode, and the default configuration) it fails and nothing
 * ever reaps anything. A node observed with 37 rooms in memory while three were
 * in use, climbing with every connection anyone had ever made.
 *
 * Long enough that a room emptied by a reconnect storm is still there when
 * people come back, and that members waiting out their own disconnect grace
 * period are not stranded.
 */
const EMPTY_ROOM_TTL = Number(process.env.EMPTY_ROOM_TTL_MS || 10 * 60 * 1000);

/**
 * Largest snapshot a client may publish.
 *
 * Snapshots are client-supplied, kept in memory for the life of the room, and
 * handed to every late joiner - so their size is another thing one client can
 * make a room pay for, in the same shape as an input flood. Nothing checked it:
 * a single 4MB snapshot took the node from 23MB of heap to 34MB, and 76MB
 * resident to 103MB, with one message.
 *
 * Two megabytes is far above anything the demos produce - the largest measured
 * was 16KB before it was trimmed, and 4.6KB after - while still leaving room
 * for an application with a genuinely large world.
 */
const MAX_SNAPSHOT_BYTES = Number(process.env.MAX_SNAPSHOT_BYTES || 2 * 1024 * 1024);

// Max age of snapshot before warning (ms) - 30 seconds
const SNAPSHOT_STALE_THRESHOLD = 30000;

/**
 * Room creation options
 */
interface RoomOptions {
  // If true, membership is removed on disconnect (multiplayer session-based mode)
  // If false, membership persists after disconnect (chat app mode)
  // Default: true (session-based mode - backwards compatible)
  removeOnDisconnect?: boolean;
}


class RoomManager {
  private rooms: Map<string, Room> = new Map();
  private sequenceCounters: Map<string, number> = new Map();
  private pendingInputs: Map<string, NetworkInput[]> = new Map(); // Buffer for inputs that arrive before room exists
  private pendingRelayInputs: Map<string, NetworkInput[]> = new Map(); // Inputs that need to be relayed to authority
  private pendingPeerHistory: Map<string, NetworkInput[]> = new Map(); // Already-sequenced history from a peer, arrived before the room existed
  private maintenanceTimer: NodeJS.Timeout | null = null;
  // Per-room hash voting. Named for partitions, which is the part of it that
  // does not run: the collector stores partition data nobody reads, and what
  // this is actually used for is the per-frame majority that desync detection
  // and snapshot acceptance both depend on. See binary-protocol.ts.
  private partitionCollectors: Map<string, PartitionCollector> = new Map();
  /** Votes refused for claiming a frame the room is nowhere near. */
  private rejectedStateHashes = 0;
  /** Snapshots refused for claiming a frame the room has not reached. */
  private rejectedSnapshotFrames = 0;

  constructor() {
    // Start periodic maintenance
    this.startMaintenance();
  }

  private startMaintenance() {
    if (this.maintenanceTimer) return;

    this.maintenanceTimer = setInterval(() => {
      // Attributed rather than left to the process-level net: a maintenance
      // sweep that throws every tick should say which sweep it was, not just
      // that something somewhere did.
      try {
        this.runMaintenance();
      } catch (err) {
        console.error('[MAINTENANCE] sweep threw:', err);
      }
    }, MAINTENANCE_INTERVAL);
  }

  private runMaintenance() {
    const now = Date.now();

    // Helper to check if input is a lifecycle input (join/leave/disconnect/reconnect)
    // These must be preserved for late joiners to determine authority chain
    const isLifecycleInput = (input: NetworkInput) => {
      const type = input.type || (input as any).data?.type;
      return type === 'join' || type === 'reconnect' || type === 'leave' || type === 'disconnect';
    };

    // Drop rooms nobody is in. A room still holding members is left alone: those
    // are disconnected players inside their reconnect window, and the member
    // reaper will clear them first if they never return.
    for (const [roomId, room] of this.rooms) {
      if (room.clients.size > 0 || room.members.size > 0) {
        room.emptySince = undefined;
        continue;
      }
      if (room.emptySince === undefined) { room.emptySince = now; continue; }
      if (now - room.emptySince < EMPTY_ROOM_TTL) continue;
      console.log(
        `[MAINTENANCE] Dropping room ${roomId}: empty for ` +
        `${Math.round((now - room.emptySince) / 1000)}s`
      );
      this.deleteRoom(roomId);
    }

    // Tick state for rooms that are not here. See dropOrphanedTickStates.
    const orphaned = dropOrphanedTickStates((roomId) => this.rooms.has(roomId));
    if (orphaned > 0) {
      console.log(`[MAINTENANCE] Dropped ${orphaned} tick state(s) for rooms this node does not have`);
    }

    for (const [roomId, room] of this.rooms) {
      // Prune game inputs older than snapshot - they're already reflected in snapshot state
      // Late joiners get: snapshot + inputs since snapshot = full state
      // CRITICAL: Lifecycle inputs (join/leave/etc) are PRESERVED - late joiners need these
      // to determine the authority chain (first joiner = authority)
      // Prune on the same basis the catch-up selector uses. Pruning by seq while
      // serving by frame would drop history a late joiner still needs; and since
      // an inferred seq is a lower bound that can legitimately be 0, gating on
      // `seq > 0` would silently latch pruning off forever and let the room grow
      // to the hard cap below.
      if (room.snapshot) {
        const byFrame = this.catchUpByFrame(room);
        const snapshotSeq = room.snapshot.seq || 0;
        const snapshotFrame = room.snapshotFrame || 0;

        if (byFrame || snapshotSeq > 0) {
          const beforeCount = room.inputs.length;

          // Keep lifecycle inputs (for authority chain) AND anything the snapshot
          // does not already account for.
          room.inputs = room.inputs.filter(e => {
            if (isLifecycleInput(e)) return true;
            if (byFrame) return e.frame === undefined || e.frame > snapshotFrame;
            return (e.seq || 0) > snapshotSeq;
          });

          const pruned = beforeCount - room.inputs.length;
          if (pruned > 0) {
            const lifecycleCount = room.inputs.filter(isLifecycleInput).length;
            const basis = byFrame ? `frame ${snapshotFrame}` : `seq ${snapshotSeq}`;
            console.log(`[MAINTENANCE] Room ${roomId}: pruned ${pruned} inputs older than snapshot ${basis}, kept ${room.inputs.length} (${lifecycleCount} lifecycle)`);
          }
        }
      }

      // Hard cap on retained inputs. Snapshot pruning above only fires for rooms
      // that actually receive snapshots; without this cap a room that never
      // snapshots (or whose history is all lifecycle inputs) grows forever.
      // Drop from the front so the newest history survives.
      if (room.inputs.length > MAX_INPUTS_PER_ROOM) {
        const overflow = room.inputs.length - MAX_INPUTS_PER_ROOM;
        room.inputs.splice(0, overflow);
        console.log(`[MAINTENANCE] Room ${roomId}: dropped ${overflow} oldest inputs (cap ${MAX_INPUTS_PER_ROOM})`);
      }

      // Skip additional maintenance for empty rooms
      if (room.clients.size === 0) continue;

      // Check snapshot age (only for rooms with clients)
      const snapshotAge = now - room.snapshotTimestamp;
      if (snapshotAge > SNAPSHOT_STALE_THRESHOLD && room.inputs.length > 100) {
        console.log(`[MAINTENANCE] Room ${roomId}: snapshot is ${Math.round(snapshotAge / 1000)}s old with ${room.inputs.length} pending inputs`);
      }
    }
  }

  stopMaintenance() {
    if (this.maintenanceTimer) {
      clearInterval(this.maintenanceTimer);
      this.maintenanceTimer = null;
    }
  }

  createRoom(
    roomId: string,
    initialSnapshot: any = {},
    existingInputs: NetworkInput[] = [],
    isAuthority: boolean = true,
    options: { removeOnDisconnect?: boolean } = {}
  ): Room {
    // Default to empty snapshot if undefined
    const snapshot = initialSnapshot ?? {};
    // CRITICAL: Check if room already exists to prevent race condition overwrite
    const existing = this.rooms.get(roomId);
    if (existing) {
      // If room exists but has NO clients and new joiner is authority,
      // reset the stale snapshot to prevent late joiners from receiving ancient state
      if (existing.clients.size === 0 && isAuthority) {
        const snapshotAge = Date.now() - existing.snapshotTimestamp;
        console.log(`[ROOM] Empty room ${roomId} exists with ${Math.round(snapshotAge / 1000)}s old snapshot, resetting for new authority`);
        existing.snapshot = snapshot;
        existing.snapshotHash = this.hashSnapshot(snapshot);
        existing.snapshotTimestamp = Date.now();
        existing.snapshotSize = JSON.stringify(snapshot).length;
        existing.inputs = []; // Clear old inputs too
        existing.isAuthority = isAuthority;
        this.sequenceCounters.set(roomId, 0);
        this.drainPeerHistory(existing);
        return existing;
      }
      // Being told to host a room this node already has as a replica means the
      // authority went away and central promoted this one. Take the role.
      //
      // Returning the room unchanged left it a replica forever, and a replica
      // does not tick - so a room whose authority died froze for every player
      // on it while every one of them was still connected and reporting
      // healthy. It was the players landing here that made this the node to
      // promote, and refusing the promotion is what stopped the world.
      //
      // The state is deliberately kept. The empty-room branch above resets a
      // stale snapshot because nobody is depending on it; here there are
      // clients mid-game whose world this is, and replacing it under them is
      // the desync that promotion is supposed to avoid.
      if (isAuthority && !existing.isAuthority) {
        console.log(`[ROOM] Promoting ${roomId} to authority on this node, keeping its ${existing.clients.size} clients and current state`);
        existing.isAuthority = true;
      }
      console.log(`[WARN] createRoom called for existing room ${roomId} with ${existing.clients.size} clients, returning existing room`);
      this.drainPeerHistory(existing);
      return existing;
    }

    // Get any buffered inputs that arrived before room was created
    const bufferedInputs = this.pendingInputs.get(roomId) || [];
    this.pendingInputs.delete(roomId);

    if (bufferedInputs.length > 0) {
      if (isAuthority) {
        // Authority: merge buffered inputs with existing inputs
        const existingSeqs = new Set(existingInputs.map(e => e.seq));
        const newInputs = bufferedInputs.filter(e => !existingSeqs.has(e.seq));
        existingInputs = [...existingInputs, ...newInputs].sort((a, b) => a.seq - b.seq);
        console.log(`[BUFFER] Authority room ${roomId} merged ${newInputs.length} buffered inputs`);
      } else {
        // Replica: store buffered inputs separately for relaying to authority
        // They'll be picked up by getPendingRelayInputs()
        this.pendingRelayInputs.set(roomId, bufferedInputs);
        console.log(`[BUFFER] Replica room ${roomId} has ${bufferedInputs.length} inputs to relay`);
      }
    }
    const snapshotHash = this.hashSnapshot(snapshot);

    // Calculate initial sequence counter from existing inputs
    const maxSeq = existingInputs.reduce((max, input) => Math.max(max, input.seq || 0), 0);

    const snapshotSize = JSON.stringify(snapshot).length;

    // Default removeOnDisconnect to true (session-based mode) for backwards compatibility
    const removeOnDisconnect = options.removeOnDisconnect ?? true;

    const room: Room = {
      id: roomId,
      snapshot: snapshot,
      snapshotHash,
      snapshotTimestamp: Date.now(),
      snapshotSize,
      inputs: existingInputs,
      clients: new Map(),
      members: new Map(),
      isAuthority,
      removeOnDisconnect,
      departedClientState: new Map(),
      pendingResyncClients: new Set(),
    };

    this.rooms.set(roomId, room);
    // History that arrived from a peer while this room was still being built.
    this.drainPeerHistory(room);
    this.sequenceCounters.set(roomId, maxSeq);

    console.log(`[ROOM] Created room ${roomId}, removeOnDisconnect=${removeOnDisconnect}`);
    return room;
  }

  getRoom(roomId: string): Room | undefined {
    return this.rooms.get(roomId);
  }

  /**
   * Sizes of the collections that outlive a single tick.
   *
   * For finding where retained memory is going without guessing. Every entry
   * here is something that has either caused a leak in this file before or is
   * shaped like it could: keyed by room or by client, added to on a hot path,
   * and cleaned up somewhere else.
   */
  debugSizes(): Record<string, number> {
    let desyncReports = 0, members = 0, masterClients = 0, collectorFrames = 0, hashVotes = 0;
    let departedState = 0, pendingResync = 0, roomInputs = 0;
    for (const room of this.rooms.values()) {
      desyncReports += room.desyncReports?.length ?? 0;
      members += room.members?.size ?? 0;
      masterClients += room.masterClientList?.size ?? 0;
      // Two that were not counted, found while hunting a leak that every other
      // counter here said did not exist. Departed clients' state is written on
      // disconnect and nothing was seen removing it; the resync set is small but
      // is another per-client thing with no obvious bound. Counting them is
      // cheap, and "the leak is not in any of these" is only worth saying about
      // structures that are actually in the list.
      departedState += room.departedClientState?.size ?? 0;
      pendingResync += room.pendingResyncClients?.size ?? 0;
      roomInputs += room.inputs?.length ?? 0;
    }
    for (const collector of this.partitionCollectors.values()) {
      const sizes = collector.debugSizes();
      collectorFrames += sizes.frames;
      hashVotes += sizes.hashVotes;
    }
    return {
      rooms: this.rooms.size,
      sequenceCounters: this.sequenceCounters.size,
      pendingInputs: this.pendingInputs.size,
      pendingRelayInputs: this.pendingRelayInputs.size,
      pendingPeerHistory: this.pendingPeerHistory.size,
      partitionCollectors: this.partitionCollectors.size,
      collectorFrames,
      hashVotes,
      desyncReports,
      members,
      masterClients,
      departedState,
      pendingResync,
      roomInputs,
      pendingPeerInputs: pendingPeerInputCount(),
      tickStates: tickStateCount(),
      // Climbing means somebody is voting on frames the room is not near.
      // Ordinary play does not produce these at all.
      rejectedStateHashes: this.rejectedStateHashes,
      // Bytes written to client sockets that have not gone out yet.
      //
      // Every tick is sent to every client whether or not that client is
      // draining its socket, and nothing anywhere checks bufferedAmount. A
      // client on a slow link cannot keep up with a fixed-rate broadcast, so
      // the backlog lives in this process's memory - which makes an ordinary
      // bad network, not an attacker, an unbounded growth vector. Reported so
      // the question is answerable from outside.
      clientBufferBytes: this.clientBufferBytes(),
      clientBufferWorst: this.clientBufferWorst(),
      rejectedSnapshotFrames: this.rejectedSnapshotFrames,
    };
  }

  getAllRooms(): Room[] {
    return Array.from(this.rooms.values());
  }

  /**
   * Delete a room and all its state.
   * Closes any remaining client connections and cleans up tick state.
   */
  deleteRoom(roomId: string): boolean {
    const room = this.rooms.get(roomId);
    if (!room) return false;

    // The room's tick state goes with the room.
    //
    // Stopping a tick deliberately keeps that state, so a client rejoining a
    // quiet room resumes at the right frame - but that is meant to outlive the
    // tick, not the room. Nothing cleaned it up on deletion except one HTTP
    // route, and the empty-room reaper does not go through that route, so every
    // room this node ever reaped left its tick state behind holding its last
    // queued inputs. Measured: a suite run took the node to 34 rooms and 37 tick
    // states, the reaper took the rooms back down to 3, and the tick states
    // stayed at 37 - 34 of them belonging to rooms that no longer existed.
    cleanupRoomTick(roomId);

    // Close any remaining client websockets
    for (const [, client] of room.clients) {
      try {
        client.socket.close(1000, 'Room deleted');
      } catch {
        // Socket may already be closed
      }
    }

    this.rooms.delete(roomId);
    this.sequenceCounters.delete(roomId);
    // Drop every other per-room map too, otherwise deleting a room leaks its
    // partition collector, buffered inputs and pending relay inputs forever.
    this.cleanupPartitionCollector(roomId);
    this.pendingInputs.delete(roomId);
    this.pendingRelayInputs.delete(roomId);
    console.log(`[ROOM] Deleted room ${roomId}`);
    return true;
  }

  addClient(
    roomId: string,
    clientId: string,
    socket: WebSocket,
    nodeId: string,
    isMuted: boolean = false,
    metadata?: Record<string, any>
  ): boolean {
    const room = this.rooms.get(roomId);
    if (!room) return false;

    room.clients.set(clientId, {
      id: clientId,
      socket,
      joinedAt: Date.now(),
      nodeId,
      isMuted,
      metadata,
      initialStateReceived: false
    });

    // If this is the authority node, update the master client list
    if (room.isAuthority) {
      if (!room.masterClientList) {
        room.masterClientList = new Map();
      }
      room.masterClientList.set(clientId, {
        clientId,
        nodeId,
        isMuted,
        metadata,
        joinedAt: Date.now()
      });
    }

    return true;
  }

  removeClient(roomId: string, clientId: string): boolean {
    const room = this.rooms.get(roomId);
    if (!room) return false;

    // Remove from local clients
    const removed = room.clients.delete(clientId);

    // If this is the authority node, also remove from master client list
    if (room.isAuthority && room.masterClientList) {
      room.masterClientList.delete(clientId);
    }

    // An empty room has no simulation to be a version of. Holding on to the old
    // fingerprint would mean a room that outlived a deploy - nobody in it, but
    // not yet cleaned up - greeting every new client with "you are stale", which
    // is precisely the false alarm this check exists to avoid.
    if (room.clients.size === 0) {
      room.simVersion = undefined;
    }

    return removed;
  }

  // Mark client as having received INITIAL_STATE (ready for TICK broadcasts)
  setClientInitialStateReceived(roomId: string, clientId: string): void {
    const room = this.rooms.get(roomId);
    const client = room?.clients.get(clientId);
    if (client) {
      client.initialStateReceived = true;
    }
  }

  // Master client list management (authority node only)
  getMasterClientList(roomId: string): Map<string, any> | undefined {
    const room = this.rooms.get(roomId);
    return room?.masterClientList;
  }

  updateMasterClientList(roomId: string, clientList: Map<string, any>): boolean {
    const room = this.rooms.get(roomId);
    if (!room || !room.isAuthority) return false;

    room.masterClientList = clientList;
    return true;
  }

  addToMasterClientList(
    roomId: string,
    clientId: string,
    nodeId: string,
    isMuted: boolean,
    metadata?: Record<string, any>
  ): boolean {
    const room = this.rooms.get(roomId);
    if (!room || !room.isAuthority) return false;

    if (!room.masterClientList) {
      room.masterClientList = new Map();
    }

    room.masterClientList.set(clientId, {
      clientId,
      nodeId,
      isMuted,
      metadata,
      joinedAt: Date.now()
    });

    return true;
  }

  removeFromMasterClientList(roomId: string, clientId: string): boolean {
    const room = this.rooms.get(roomId);
    if (!room || !room.isAuthority || !room.masterClientList) return false;

    return room.masterClientList.delete(clientId);
  }

  // Remove all clients from a specific node (used when a replica node goes down)
  removeClientsFromNode(roomId: string, nodeId: string): string[] {
    const room = this.rooms.get(roomId);
    if (!room || !room.isAuthority || !room.masterClientList) return [];

    const removedClientIds: string[] = [];

    for (const [clientId, entry] of room.masterClientList.entries()) {
      if (entry.nodeId === nodeId) {
        room.masterClientList.delete(clientId);
        removedClientIds.push(clientId);
      }
    }

    return removedClientIds;
  }

  // ==================== Member Management ====================
  // Members persist across disconnections. A member is only removed when they explicitly leave.
  // This enables the distinction between "disconnect" (temporary) and "leave" (permanent).

  /**
   * Add a new member to the room.
   * Called when a user joins for the first time (not reconnection).
   */
  addMember(roomId: string, odId: string, clientId: string, metadata?: any): boolean {
    const room = this.rooms.get(roomId);
    if (!room) return false;

    // Don't overwrite existing member - use setMemberStatus for reconnection
    if (room.members.has(odId)) {
      console.log(`[MEMBER] Member ${odId} already exists in room ${roomId}`);
      return false;
    }

    room.members.set(odId, {
      odId,
      odMetadata: metadata,
      clientId,
      joinedAt: Date.now(),
      status: 'connected'
    });

    console.log(`[MEMBER] Added member ${odId} to room ${roomId}`);
    return true;
  }

  /**
   * Remove a member from the room permanently.
   * Called when a user explicitly leaves (not disconnect).
   */
  removeMember(roomId: string, odId: string): boolean {
    const room = this.rooms.get(roomId);
    if (!room) return false;

    const removed = room.members.delete(odId);
    if (removed) {
      console.log(`[MEMBER] Removed member ${odId} from room ${roomId}`);
    }
    return removed;
  }

  /**
   * Get member info by their odId (user.id).
   */
  getMemberByOdId(roomId: string, odId: string): MemberInfo | undefined {
    const room = this.rooms.get(roomId);
    if (!room) return undefined;

    return room.members.get(odId);
  }

  /**
   * Check if a user is already a member of the room.
   */
  isMember(roomId: string, odId: string): boolean {
    const room = this.rooms.get(roomId);
    if (!room) return false;

    return room.members.has(odId);
  }

  /**
   * Get all members of a room (both connected and disconnected).
   */
  getMembers(roomId: string): MemberInfo[] {
    const room = this.rooms.get(roomId);
    if (!room) return [];

    return Array.from(room.members.values());
  }

  /**
   * Update member's connection status and clientId.
   * Called on disconnect (status='disconnected') or reconnect (status='connected').
   *
   * @param clientId which connection this is about. Setting 'connected' hands
   *   the member to that connection. Setting 'disconnected' asks on behalf of
   *   it, and is refused when the member has since been handed to another: a
   *   refresh races the old socket's close against the new socket's join, and
   *   when the join wins, the close that follows must not demote a member whose
   *   live connection is the newer one. It did, and with a grace behind it the
   *   sweep then removed a player who was connected and playing.
   */
  setMemberStatus(roomId: string, odId: string, status: 'connected' | 'disconnected', clientId?: string): boolean {
    const room = this.rooms.get(roomId);
    if (!room) return false;

    const member = room.members.get(odId);
    if (!member) return false;

    if (status === 'disconnected' && clientId && member.clientId && member.clientId !== clientId) {
      console.log(
        `[MEMBER] Ignoring disconnect of ${odId} in room ${roomId} from ${clientId}: ` +
        `the member is held by ${member.clientId}`
      );
      return false;
    }

    member.status = status;
    if (status === 'disconnected') {
      member.disconnectedAt = Date.now();
      member.clientId = undefined;
    } else {
      member.disconnectedAt = undefined;
      // They are back, so whatever their last page did on its way out no longer
      // says anything about them. Left set, a member who unloaded and returned
      // would keep being judged on the short grace for the rest of the room.
      member.unloading = undefined;
      if (clientId) {
        member.clientId = clientId;
      }
    }

    console.log(`[MEMBER] Member ${odId} in room ${roomId} status changed to ${status}`);
    return true;
  }

  /**
   * Note that this member's page told us it was unloading.
   *
   * Recorded against the member rather than the connection because the socket
   * closing is what happens next: by the time the disconnect is processed the
   * connection is gone, and the only thing left to carry the reason is the
   * membership it belonged to.
   */
  setMemberUnloading(roomId: string, odId: string): boolean {
    const room = this.rooms.get(roomId);
    if (!room) return false;

    const member = room.members.get(odId);
    if (!member) return false;

    member.unloading = true;
    return true;
  }

  /**
   * Update room options (for admin use).
   * @param roomId - Room ID
   * @param options - Options to update
   */
  setRoomOptions(roomId: string, options: { removeOnDisconnect?: boolean }): boolean {
    const room = this.rooms.get(roomId);
    if (!room) return false;

    if (options.removeOnDisconnect !== undefined) {
      room.removeOnDisconnect = options.removeOnDisconnect;
      console.log(`[ROOM] Room ${roomId} removeOnDisconnect changed to ${options.removeOnDisconnect}`);
    }

    return true;
  }

  /**
   * Get room options.
   */
  getRoomOptions(roomId: string): { removeOnDisconnect: boolean } | undefined {
    const room = this.rooms.get(roomId);
    if (!room) return undefined;

    return {
      removeOnDisconnect: room.removeOnDisconnect
    };
  }

  // Add input with sequence number (authority node only)
  // Seq is assigned in arrival order - this IS the deterministic order.
  // All clients receive inputs in seq order via TICK, so they all process identically.
  addInput(roomId: string, input: NetworkInput): number | null {
    const room = this.rooms.get(roomId);
    if (!room) return null;

    // "Authority node only" was a comment, and every caller but one honoured
    // it. The one that did not was the resync handler, which numbered an input
    // from whichever node served the request - so a replica handed out
    // sequence numbers from its own counter, and the two nodes of a room ended
    // up numbering different inputs the same. Measured while chasing the
    // multi-node join bug: 104 of the replica's sequence numbers did not exist
    // on the authority at all.
    //
    // Nothing downstream survives that. Catch-up selection filters by seq, the
    // pruner drops by seq, and peers de-duplicate by seq, so a seq that means
    // two things quietly corrupts all three.
    //
    // Checked here rather than trusted to the callers, because that is the
    // arrangement that just failed. A replica with something to sequence has to
    // relay it to the authority; there is no correct local answer.
    if (!room.isAuthority) {
      console.warn(
        `[SEQ] Refusing to number an input for room ${roomId}: this node is a replica. ` +
          `Sequence numbers are the authority's to assign - relay it instead.`
      );
      return null;
    }

    const currentSeq = this.sequenceCounters.get(roomId) || 0;
    const nextSeq = currentSeq + 1;

    input.seq = nextSeq;
    room.inputs.push(input);

    this.sequenceCounters.set(roomId, nextSeq);
    this.noteClientInput(room, input.clientId);

    return nextSeq;
  }

  /**
   * Record that a client is still sending, so "connected" and "participating"
   * can be told apart. Every input the authority accepts passes through here,
   * whichever node it arrived at, so this is the one place that sees them all.
   */
  /**
   * Record that a client published a snapshot.
   *
   * A room is meant to elect exactly one publisher. For a long time three of
   * five clients in every demo room were publishing, each paying three to five
   * times the outbound bandwidth of the ones that were not, and nothing here
   * could have told anybody - the node accepted every copy without noticing how
   * many senders they came from. Counting per client makes the invariant
   * checkable from outside instead of being a property nobody can see.
   */
  noteSnapshotPublisher(roomId: string, clientId?: string): void {
    const room = this.rooms.get(roomId);
    if (!room || !clientId || !room.masterClientList) return;
    const entry = room.masterClientList.get(clientId);
    if (!entry) return;
    entry.snapshotsPublished = (entry.snapshotsPublished || 0) + 1;
  }

  private noteClientInput(room: Room, clientId?: string): void {
    if (!clientId || !room.masterClientList) return;
    const entry = room.masterClientList.get(clientId);
    if (!entry) return;
    entry.lastInputAt = Date.now();
    entry.inputsReceived = (entry.inputsReceived || 0) + 1;
  }

  // Store input with existing sequence number (non-authority nodes)
  storeInput(roomId: string, input: NetworkInput): boolean {
    const room = this.rooms.get(roomId);
    if (!room) return false;

    // CRITICAL: Check for duplicate sequence numbers to prevent desync
    const existing = room.inputs.find(e => e.seq === input.seq);
    if (existing) {
      // The same input, arriving again carrying the frame it was broadcast in.
      //
      // A replica holds a client's input before the authority has sequenced it,
      // with no frame yet. The authority's broadcast then brings it back
      // stamped, and dropping that as a duplicate left the local copy without a
      // frame forever - which every catch-up filter reads as "not broadcast
      // yet" and skips. The input is in the room and invisible to anybody
      // joining afterwards.
      //
      // Measured on a two-node room: at the moment a client joined the replica,
      // the authority held two inputs for frame 41 and the replica held one.
      // That client restored the snapshot at frame 40, replayed from 41 without
      // the missing input, and disagreed with the room from its first frame -
      // one other player a fraction behind where everybody else had them, every
      // other value identical.
      if (existing.frame === undefined && input.frame !== undefined) {
        (existing as any).frame = input.frame;
        return true;
      }
      // Already have this input, skip
      return false;
    }

    room.inputs.push(input);

    // Update sequence counter to track latest received
    const currentSeq = this.sequenceCounters.get(roomId) || 0;
    if (input.seq > currentSeq) {
      this.sequenceCounters.set(roomId, input.seq);
    }

    return true;
  }

  /**
   * Buffer already-sequenced history that arrived from a peer before this node
   * had built the room.
   *
   * This is the mirror image of bufferInput. That one buffers a *client's* new
   * input so it can be relayed onward, and deliberately drops anything already
   * carrying a seq. Broadcast history from the authority always carries a seq,
   * so routing it through bufferInput discarded it outright: a replica that
   * received a tick a moment before its room object existed lost that slice of
   * history for good, and its own clients never saw those joins. Membership
   * would then differ between nodes for the rest of the room's life.
   */
  bufferPeerHistory(roomId: string, input: NetworkInput): void {
    if (!this.pendingPeerHistory.has(roomId)) {
      this.pendingPeerHistory.set(roomId, []);
    }
    const buffer = this.pendingPeerHistory.get(roomId)!;
    // The same input can be relayed more than once; keep one copy per seq.
    if (input.seq !== undefined && input.seq > 0 && buffer.some(i => i.seq === input.seq)) {
      return;
    }
    buffer.push(input);
  }

  /** Fold any buffered peer history into a room's inputs, in seq order. */
  private drainPeerHistory(room: Room): void {
    const buffered = this.pendingPeerHistory.get(room.id);
    if (!buffered || buffered.length === 0) return;
    this.pendingPeerHistory.delete(room.id);

    const seen = new Set(room.inputs.map(i => i.seq));
    const fresh = buffered.filter(i => !seen.has(i.seq));
    if (fresh.length === 0) return;

    room.inputs.push(...fresh);
    room.inputs.sort((a, b) => (a.seq || 0) - (b.seq || 0));
    console.log(`[BUFFER] Room ${room.id} folded in ${fresh.length} buffered peer inputs`);
  }

  // Buffer input for a room that doesn't exist yet (will be applied when room is created)
  // CRITICAL: Only buffer inputs WITHOUT sequence numbers - those are new inputs from clients
  // Inputs WITH sequence numbers came from BROADCAST_INPUTS and are already tracked by authority
  bufferInput(roomId: string, input: NetworkInput): void {
    if (input.seq !== undefined && input.seq > 0) {
      return; // Skip already-sequenced inputs to prevent duplicates
    }
    if (!this.pendingInputs.has(roomId)) {
      this.pendingInputs.set(roomId, []);
    }
    this.pendingInputs.get(roomId)!.push(input);
  }

  // Get and consume inputs that need to be relayed to authority (for replica rooms)
  consumePendingRelayInputs(roomId: string): NetworkInput[] {
    const inputs = this.pendingRelayInputs.get(roomId) || [];
    this.pendingRelayInputs.delete(roomId);
    return inputs;
  }

  // Backwards compatibility aliases (will be removed)
  addEvent(roomId: string, event: NetworkInput): number | null { return this.addInput(roomId, event); }
  storeEvent(roomId: string, event: NetworkInput): boolean { return this.storeInput(roomId, event); }
  bufferEvent(roomId: string, event: NetworkInput): void { this.bufferInput(roomId, event); }
  consumePendingRelayEvents(roomId: string): NetworkInput[] { return this.consumePendingRelayInputs(roomId); }

  // Track departed client state (for late joiner catch-up)
  // Network stores this opaquely - application decides what to put in it
  saveDepartedClientState(roomId: string, clientId: string, state: any): void {
    const room = this.rooms.get(roomId);
    if (!room) return;

    if (!room.departedClientState) {
      room.departedClientState = new Map();
    }
    room.departedClientState.set(clientId, state);
    console.log(`[ROOM] Saved departed client ${clientId} state for late joiners`);
  }

  getDepartedClientState(roomId: string): Map<string, any> | undefined {
    const room = this.rooms.get(roomId);
    return room?.departedClientState;
  }

  /**
   * Store a snapshot for a room.
   *
   * `options.seqInferred` is how a replicated snapshot carries the origin node's
   * answer to "did a client vouch for this seq, or did a server guess it?".
   * Without it the receiving node sees a snapshot that already has a numeric seq
   * and concludes a client supplied it - laundering a guess into a claim, which
   * is exactly the desync the inference exists to avoid, on every node
   * downstream of the authority.
   */
  updateSnapshot(roomId: string, snapshot: any, hash: string, options: { seqInferred?: boolean } = {}): boolean {
    const room = this.rooms.get(roomId);
    if (!room) return false;

    // Size it once, before anything is kept, and use that same number for the
    // stored size - this used to be stringified a second time purely to record
    // a length, which for a large snapshot is megabytes of allocation per
    // update thrown straight away.
    let encodedSize = 0;
    try {
      encodedSize = JSON.stringify(snapshot).length;
    } catch {
      console.warn(`[SNAPSHOT] Room ${roomId}: snapshot could not be encoded; rejecting`);
      return false;
    }
    if (encodedSize > MAX_SNAPSHOT_BYTES) {
      console.warn(
        `[SNAPSHOT] Room ${roomId}: rejecting a ${Math.round(encodedSize / 1024)}KB snapshot, ` +
        `over the ${Math.round(MAX_SNAPSHOT_BYTES / 1024)}KB limit; keeping the previous one`
      );
      return false;
    }

    // Use frame for ordering (simulation frame) but preserve seq (input sequence) separately
    const snapshotFrame = snapshot.frame || 0;
    const currentFrame = room.snapshot?.frame || 0;

    // A snapshot cannot come from a frame the room has not reached.
    //
    // This number is written by the client and was only ever checked for being
    // too old. Too new is the dangerous direction: it is kept as
    // room.snapshotFrame, and the maintenance sweep prunes the input history
    // against it, on the rule that everything at or before the snapshot's frame
    // is already baked into it. A snapshot claiming frame 2^31 therefore
    // discards the history, and keeps it discarded - every honest snapshot that
    // follows is older than the stored frame and refused by the check below.
    // Measured before this guard: one message left the room holding
    // snapshotFrame 2147483648.
    //
    // The same window the hash votes use, and for the same reason: a client
    // simulates frames the server has already broadcast, so it is never
    // meaningfully in front of the room.
    const roomFrame = getCurrentFrame(roomId);
    if (roomFrame > 0 && snapshotFrame > roomFrame + HASH_VOTE_AHEAD) {
      console.warn(
        `[SNAPSHOT] Room ${roomId}: rejecting a snapshot claiming frame ${snapshotFrame} ` +
        `while the room is on ${roomFrame}`
      );
      this.rejectedSnapshotFrames++;
      return false;
    }

    // Only update if incoming snapshot is from a newer simulation frame
    if (snapshotFrame > 0 && currentFrame > 0 && snapshotFrame < currentFrame) {
      return false;
    }

    // CRITICAL: snapshot.seq is the input sequence number a late joiner filters
    // on - everything at or below it is assumed to be baked into the snapshot
    // already, so it is never sent. Getting this wrong in the high direction
    // silently deletes inputs from the late joiner's history and desyncs it
    // permanently, with nothing reporting an error.
    //
    // The client should supply it. When it does not, we can only infer it, and
    // the inference has to be a LOWER bound. The naive "highest seq in the
    // room" is an upper bound: a snapshot describing frame N arrives over the
    // network some milliseconds later, by which time frames N+1.. have already
    // been sequenced and broadcast, and those inputs are demonstrably NOT in
    // it. Counting only inputs already broadcast at or before the snapshot's
    // own frame gives the correct value and keeps the bandwidth saving.
    //
    // Regression coverage: e2e/late-join.spec.ts.
    let inferred = false;
    if (snapshot.seq === undefined || snapshot.seq === null) {
      // Iterate rather than Math.max(...spread): room.inputs can hold thousands
      // of entries and spreading them as arguments overflows the stack.
      let maxInputSeq = 0;
      for (const input of room.inputs) {
        // Not yet broadcast, so no client has applied it - cannot be in the snapshot.
        if (input.frame === undefined) continue;
        // Broadcast after the snapshot's frame, so it is not in the snapshot either.
        if (snapshotFrame > 0 && input.frame > snapshotFrame) continue;
        const seq = input.seq || 0;
        if (seq > maxInputSeq) maxInputSeq = seq;
      }
      // Without a frame to anchor against there is nothing safe to infer, so
      // fall back to sending the whole history rather than guessing high.
      snapshot.seq = snapshotFrame > 0 ? maxInputSeq : 0;
      inferred = true;
    }

    room.snapshot = snapshot;
    room.snapshotHash = hash;
    room.snapshotFrame = snapshotFrame;
    // A replicated snapshot carries the origin's verdict; anything else is a
    // fresh upload from a client, so what we just worked out above stands.
    //
    // This deliberately does not try to guess from the previous frame. An
    // earlier version compared `snapshotFrame` against `room.snapshotFrame`
    // after already overwriting it, so the comparison was always true and the
    // flag became a one-way latch: once inferred, every later snapshot for that
    // room was treated as inferred too, even ones carrying a real client seq.
    room.snapshotSeqInferred = options.seqInferred !== undefined ? options.seqInferred : inferred;
    room.snapshotTimestamp = Date.now();
    room.snapshotSize = encodedSize;

    // Note: We keep all inputs and let the MAX_INPUTS_PER_ROOM limit in runMaintenance() handle memory.
    // Late joiners will filter based on seq when replaying.

    console.log(`[SNAPSHOT] Room ${roomId}: stored at frame=${snapshotFrame}, seq=${snapshot.seq}, isAuthority=${room.isAuthority}, kept ${room.inputs.length} inputs, size=${room.snapshotSize}`);

    return true;
  }

  /**
   * Update snapshot with binary data (opaque bytes, server doesn't interpret)
   */
  updateBinarySnapshot(roomId: string, binaryData: Buffer): boolean {
    const room = this.rooms.get(roomId);
    if (!room) return false;

    // Store binary snapshot as base64 for JSON compatibility
    room.binarySnapshot = binaryData;
    room.snapshotTimestamp = Date.now();
    room.snapshotSize = binaryData.length;

    console.log(`[SNAPSHOT] Room ${roomId}: stored binary snapshot, ${binaryData.length} bytes`);
    return true;
  }

  validateSnapshotHash(snapshot: any, hash: string): boolean {
    // Accept any hash since sync is the goal, not security
    // Different clients may use different hash algorithms
    return Boolean(hash && hash.length > 0);
  }

  /**
   * Would this snapshot hand the room a world it does not agree with?
   *
   * Snapshots are what late joiners restore from and what a resync sends to a
   * client asking to be repaired, so whoever publishes them is the room's
   * source of truth. Nothing checked that they were still part of the room:
   * election is by lowest id, a client that diverges keeps that id, and a
   * diverged publisher goes on publishing its own broken world every twenty
   * ticks. Observed with the publisher deliberately corrupted - it asked to be
   * resynced five times, was resynced five times, and stayed wrong, because
   * what it was being repaired with was the snapshot it had published itself.
   *
   * The room already computes a consensus hash per frame in order to detect
   * divergence, so the check is to compare the two. Not advisory: a client
   * could as easily lie about its state as be wrong about it, and this is the
   * one place where one client's word becomes everybody's starting point.
   *
   * Undecidable rather than false when there is no consensus yet for that
   * frame - early frames, or a room too small to have one - and the snapshot is
   * accepted, which is where this started.
   */
  snapshotDisagreesWithRoom(roomId: string, clientId: string, frame: number, claimedHash: string): boolean {
    // The snapshot's own frame, when the room has already decided it.
    if (Number.isFinite(frame) && claimedHash) {
      const majority = this.getMajorityHash(roomId, frame);
      const claimed = Number(claimedHash);
      if (majority !== null && majority !== undefined && Number.isFinite(claimed)
          && (claimed >>> 0) !== (majority >>> 0)) {
        return true;
      }
    }

    // Otherwise the publisher's recent standing, which is decidable now.
    //
    // A client publishes the frame it has just simulated, so the votes that
    // would judge it are still arriving and the check above is undecidable
    // exactly when it matters - measured, the corrupt snapshot was accepted
    // every time and a client restoring from it stayed broken. A client that
    // has diverged disagrees on the frames behind it too, and those are
    // settled.
    const room = this.rooms.get(roomId);
    const collector = room ? this.partitionCollectors.get(roomId) : null;
    if (!collector) return false;
    return collector.clientOutOfStep(clientId);
  }

  hashSnapshot(snapshot: any): string {
    // Handle undefined/null snapshots
    const data = JSON.stringify(snapshot ?? {});
    return crypto.createHash('sha256').update(data).digest('hex');
  }

  // Public access to hash function for peer-handler
  public computeHash(snapshot: any): string {
    return this.hashSnapshot(snapshot);
  }

  getInputsSince(roomId: string, seq: number): NetworkInput[] {
    const room = this.rooms.get(roomId);
    if (!room) return [];

    return room.inputs.filter(input => input.seq > seq);
  }

  // Backwards compatibility alias
  getEventsSince(roomId: string, sequenceNumber: number): NetworkInput[] {
    return this.getInputsSince(roomId, sequenceNumber);
  }

  setAuthority(roomId: string, isAuthority: boolean): boolean {
    const room = this.rooms.get(roomId);
    if (!room) return false;

    const wasAuthority = room.isAuthority;
    room.isAuthority = isAuthority;

    // Log authority changes
    if (wasAuthority !== isAuthority) {
      console.log(`[AUTHORITY] Room ${roomId} authority changed: ${wasAuthority} -> ${isAuthority}`);
      // Log stack trace to find the caller
      console.log(new Error().stack?.split('\n').slice(1, 5).join('\n'));
    }
    return true;
  }

  /** Total bytes queued on every client socket this node holds. */
  clientBufferBytes(): number {
    let total = 0;
    for (const room of this.rooms.values()) {
      for (const client of room.clients.values()) {
        total += (client.socket as any)?.bufferedAmount || 0;
      }
    }
    return total;
  }

  /** The worst single client's queued bytes. */
  clientBufferWorst(): number {
    let worst = 0;
    for (const room of this.rooms.values()) {
      for (const client of room.clients.values()) {
        const n = (client.socket as any)?.bufferedAmount || 0;
        if (n > worst) worst = n;
      }
    }
    return worst;
  }

  getClients(roomId: string): ClientInfo[] {
    const room = this.rooms.get(roomId);
    if (!room) return [];

    return Array.from(room.clients.values());
  }

  // Update client's hash and sync status
  updateClientHash(roomId: string, clientId: string, hash: string, seq: number, frame: number): void {
    const room = this.rooms.get(roomId);
    if (!room) return;

    const client = room.clients.get(clientId);
    if (!client) return;

    client.lastHash = hash;
    client.lastHashSeq = seq;
    client.lastHashFrame = frame;
    client.lastHashTime = Date.now();

    // Determine authority hash (lowest client ID with a hash)
    const clientsWithHash = Array.from(room.clients.entries())
      .filter(([_, c]) => c.lastHash)
      .sort(([a], [b]) => a.localeCompare(b));

    if (clientsWithHash.length === 0) return;

    const authorityHash = clientsWithHash[0][1].lastHash;

    // Update sync status for all clients with hashes
    for (const [_, c] of room.clients) {
      if (c.lastHash) {
        c.isSynced = c.lastHash === authorityHash;
      }
    }
  }

  // Get sync status for dashboard
  getRoomSyncStatus(roomId: string): { clientId: string; isSynced: boolean; hash: string; seq: number; frame: number }[] {
    const room = this.rooms.get(roomId);
    if (!room) return [];

    return Array.from(room.clients.values())
      .filter(c => c.lastHash)
      .map(c => ({
        clientId: c.id,
        isSynced: c.isSynced || false,
        hash: c.lastHash || '',
        seq: c.lastHashSeq || 0,
        frame: c.lastHashFrame || 0
      }));
  }

  // Get count of synced clients
  getSyncedClientCount(roomId: string): number {
    const room = this.rooms.get(roomId);
    if (!room) return 0;

    return Array.from(room.clients.values())
      .filter(c => c.isSynced)
      .length;
  }

  // ==========================================
  // Distributed State Sync - Partition Collection
  // ==========================================

  /**
   * Get or create the partition collector for a room.
   */
  getPartitionCollector(roomId: string): PartitionCollector {
    let collector = this.partitionCollectors.get(roomId);
    if (!collector) {
      collector = new PartitionCollector();
      this.partitionCollectors.set(roomId, collector);
    }
    return collector;
  }

  /**
   * The inputs a catching-up client must replay on top of the stored snapshot.
   *
   * Every path that serves a snapshot - JOIN, CREATE_ROOM onto an existing room,
   * and resync - has to answer this identically. They used to each inline their
   * own filter, and they drifted: one of them kept filtering by seq after the
   * others moved to frame, so joiners arriving through it silently lost history.
   *
   * The rule: if the client told us which input sequence its snapshot covers,
   * that is authoritative and we filter on seq. If the server had to guess, the
   * guess is a lower bound and must not be used to *exclude* anything, so we
   * filter on the snapshot's own frame instead - which answers the question
   * exactly, because a snapshot describes the world after that frame.
   *
   * `forClientId` always gets its own join/reconnect event back regardless, so
   * the SDK can complete its connect handshake.
   */
  /**
   * Take authority for a room already held here as a replica.
   *
   * Only the role changes. The state is kept deliberately: this is called when
   * the previous authority died, and the players whose world it is are on this
   * node - replacing their state under them is the desync that promoting a
   * replica is meant to avoid.
   *
   * Returns whether anything changed, so the caller can tell "promoted" from
   * "was already the authority" and from "no such room here".
   */
  promoteToAuthority(roomId: string): boolean {
    const room = this.rooms.get(roomId);
    if (!room || room.isAuthority) return false;
    console.log(`[ROOM] Promoting ${roomId} to authority, keeping its ${room.clients.size} clients and current state`);
    room.isAuthority = true;
    return true;
  }

  /**
   * Whether a room's history is measured by frame or by sequence number.
   *
   * Asked in two places - when pruning history, and when selecting a catch-up
   * for a joining client - and they have to give the same answer. The pruner's
   * own comment says why: prune by seq while serving by frame and you drop
   * exactly the history the late joiner was about to be sent.
   *
   * It was written out twice, and that is not safe however clearly the comment
   * explains it. I changed one of them while investigating a multi-node bug and
   * left the other alone, which is precisely the failure the comment warns
   * about, introduced by someone who had just read it. One definition, two
   * callers.
   */
  private catchUpByFrame(room: any): boolean {
    return room.snapshotSeqInferred === true && (room.snapshotFrame || 0) > 0;
  }

  /**
   * The connection ids of everyone currently in the room.
   *
   * Keyed off the member record rather than the input history on purpose: a
   * client that reconnected has a new connection id, and its previous join is
   * still sitting in the history. Only the current one is a member, so stale
   * joins are excluded without having to reason about ordering.
   */
  private currentMemberClientIds(roomId: string): Set<string> {
    const room = this.rooms.get(roomId);
    const ids = new Set<string>();
    if (!room) return ids;
    for (const member of room.members.values()) {
      if (member.clientId) ids.add(member.clientId);
    }
    // A client can be connected without having become a member yet.
    for (const clientId of room.clients.keys()) ids.add(clientId);

    // Everyone in the room, not just everyone on this node.
    //
    // members and clients are both local: a client attached to another node is
    // in neither. So on a replica this set covered only half the room, the
    // catch-up left out the joins of the players connected elsewhere, and a
    // client that arrived could not attribute their inputs. It has nothing to
    // fall back on now that the payload carries no player id, so it dropped
    // them - silently, one in six of everything it was sent - and disagreed
    // with the room from a few frames after it joined.
    //
    // Measured: the failing client's ownership map held four connections where
    // the healthy ones held five, and the id missing from it was the client on
    // the other node. It never learned that one at all, and every input from it
    // went on the floor.
    //
    // The master client list is the room-wide view, which is exactly what this
    // question needs.
    if (room.masterClientList) {
      for (const clientId of room.masterClientList.keys()) ids.add(String(clientId));
    }
    return ids;
  }

  selectCatchUpInputs(roomId: string, forClientId?: string): NetworkInput[] {
    const room = this.rooms.get(roomId);
    if (!room) return [];

    const snapshotSeq = room.snapshot?.seq || 0;
    const snapshotFrame = room.snapshotFrame || 0;
    const byFrame = this.catchUpByFrame(room);

    const selected = room.inputs.filter((input: any) => {
      // Not broadcast yet, so no client has applied it; it will arrive by tick.
      if (input.frame === undefined) return false;

      if (forClientId) {
        const inputClientId = input.data?.clientId || input.clientId;
        const inputType = input.data?.type || input.type;
        if (inputType === 'join' || inputType === 'reconnect') {
          // The joining client's own join, which it needs to see itself in the
          // room, and the joins of everyone already here - which it needs for a
          // different reason.
          //
          // An input is attributed to the connection it arrived on rather than
          // to the player id written into its payload, because the payload is
          // written by the client and is only a claim. Clients learn which
          // connection belongs to which player from these join inputs. Someone
          // arriving now has missed every join that happened before the
          // snapshot it is starting from, so it cannot attribute any of the
          // players who were already here, and falls back to believing their
          // claims. Measured, that was 60% of inputs for the fourth client into
          // a room and 80% for the fifth.
          //
          // These are safe to hand over because they are old: they sit at or
          // before the snapshot frame, so catch-up replay skips them and the
          // simulation is untouched. They are read for membership and
          // ownership, not applied. The alternative was an out-of-band client
          // list, which is accurate but arrives outside the replicated stream -
          // and attribution feeds hashed state, so it should not depend on when
          // a message happened to land.
          if (inputClientId === forClientId) return true;
          // Only joins the newcomer's catch-up replay is guaranteed to skip.
          //
          // These are handed over to be read, not applied: the client picks
          // ownership out of them and its replay ignores them because they sit
          // at or before the snapshot it restored. A join from after that frame
          // would be replayed, and replaying a join for a player the snapshot
          // already contains re-adds them - which resets what they were
          // carrying and desyncs the room. Anything after the snapshot is
          // already being sent by the ordinary filter below, so there is
          // nothing to gain by relaxing this.
          if (snapshotFrame > 0 && input.frame <= snapshotFrame
              && inputClientId && this.currentMemberClientIds(roomId).has(inputClientId)) {
            return true;
          }
        }
      }

      if (byFrame) return input.frame > snapshotFrame;
      return (input.seq || 0) > snapshotSeq;
    });

    if (!forClientId) return selected;

    // Every input handed over must be attributable from what is handed over
    // with it.
    //
    // Inputs are applied to the connection they arrived on, and a client learns
    // which connection belongs to which player from join inputs. The rule above
    // covers the joins of everyone currently in the room, which is not the same
    // set: a player who refreshes comes back on a new connection, so their
    // previous one stops being a member while its inputs are still sitting in
    // the history being replayed. The rejoining client could not attribute
    // those and dropped them, while everybody else applied them - a divergence
    // at the first such input, and one that only appears after somebody
    // reconnects.
    //
    // It stayed hidden while payloads carried the player's own id, because
    // dropping back to that claim silently covered exactly this gap. Removing
    // the claim is what made it visible.
    const needed = new Set<string>();
    for (const input of selected as any[]) {
      const cid = input.data?.clientId || input.clientId;
      const type = input.data?.type || input.type;
      if (cid && type !== 'join' && type !== 'reconnect') needed.add(String(cid));
    }
    for (const input of selected as any[]) {
      const type = input.data?.type || input.type;
      if (type === 'join' || type === 'reconnect') {
        const cid = input.data?.clientId || input.clientId;
        if (cid) needed.delete(String(cid));
      }
    }
    if (needed.size === 0) return selected;

    // Only joins old enough that catch-up replay skips them; anything newer is
    // already in `selected`.
    const backfill = room.inputs.filter((input: any) => {
      const type = input.data?.type || input.type;
      if (type !== 'join' && type !== 'reconnect') return false;
      const cid = input.data?.clientId || input.clientId;
      return Boolean(cid && needed.has(String(cid)));
    });
    if (backfill.length) {
      console.log(`[CATCHUP] room=${roomId} adding ${backfill.length} join(s) so every input sent can be attributed`);
    }
    return backfill.concat(selected);
  }

  /**
   * Record a client's report that its state hash disagreed with the consensus.
   *
   * Bounded, in-memory and never persisted: this is a debugging aid, and a room
   * whose clients are all desyncing would otherwise grow one entry per client
   * per report forever.
   */
  recordDesyncReport(roomId: string, report: DesyncReport): void {
    const room = this.rooms.get(roomId);
    if (!room) return;
    if (!room.desyncReports) room.desyncReports = [];
    room.desyncReports.push(report);
    if (room.desyncReports.length > MAX_DESYNC_REPORTS) {
      room.desyncReports.splice(0, room.desyncReports.length - MAX_DESYNC_REPORTS);
    }
  }

  getDesyncReports(roomId: string): DesyncReport[] {
    return this.rooms.get(roomId)?.desyncReports || [];
  }

  /**
   * Check a joining client's simulation fingerprint against the room's.
   *
   * Lockstep only works if everyone runs the same rules, and nothing used to
   * check. A tab left open across a deploy reconnects, simulates by the old
   * rules and disagrees on every frame forever - which arrives as a flood of
   * desync reports that look exactly like a transport bug, and sends whoever
   * is debugging it into the network stack for an afternoon.
   *
   * The first client to arrive sets the room's expected version. Anyone who
   * disagrees is named, once, with the reason - so the answer is "that tab is
   * stale, reload it" rather than a mystery.
   *
   * Returns the mismatch if there is one, so the caller can log and record it.
   */
  checkSimVersion(
    roomId: string,
    clientId: string,
    metadata?: Record<string, any>
  ): { playerId: string; theirs: string; expected: string } | null {
    const room = this.rooms.get(roomId);
    if (!room) return null;

    const theirs = metadata?.simVersion;
    // Clients that do not report a version are not evidence of anything.
    if (typeof theirs !== 'string' || !theirs) return null;

    if (!room.simVersion) {
      room.simVersion = theirs;
      return null;
    }
    if (room.simVersion === theirs) return null;

    return {
      playerId: String(metadata?.id || clientId),
      theirs,
      expected: room.simVersion,
    };
  }

  /** True when `snapshot.seq` is a server guess rather than the client's claim. */
  isSnapshotSeqInferred(roomId: string): boolean {
    return this.rooms.get(roomId)?.snapshotSeqInferred === true;
  }

  /** The frame the stored snapshot describes, or 0 if unknown. */
  getSnapshotFrame(roomId: string): number {
    return this.rooms.get(roomId)?.snapshotFrame || 0;
  }

  /**
   * Add a STATE_HASH from a client for distributed state sync.
   */
  /** Frames and votes this one room's collector is holding, for tests. */
  collectorSizes(roomId: string): { frames: number; hashVotes: number } | null {
    const collector = this.partitionCollectors.get(roomId);
    return collector ? collector.debugSizes() : null;
  }

  /**
   * Record a client's vote on a frame, if it is a frame it could have played.
   *
   * The frame here is written by the client, and it used to be taken as given.
   * The collector keys its frame collections by it and prunes everything more
   * than a few frames behind whatever has just arrived, so a vote claiming a
   * frame far in the future deleted every real frame's votes behind it. One
   * message did it, and a message per tick with a different frame each time
   * held the room permanently clear - measured, three settled clients went from
   * about twenty consensus verdicts a second to two in ten seconds.
   *
   * Nothing breaks visibly when that happens, which is what makes it worth
   * guarding. With no votes there is no majority; a client sent no majority
   * skips its comparison entirely, by design, because a verdict from too few
   * voters is worse than none. So the room stops checking anybody, quietly, and
   * a client running a modified world stops being caught. It relays to peers
   * too, so one client blinds every node the room is on.
   *
   * The votes also accumulate: each claimed frame is lower than the last, so it
   * is never behind the prune window and never removed. Ten seconds of it left
   * the collector holding 500 frames of votes for frames nobody simulated.
   *
   * So a vote is only accepted for a frame near the one the room is actually
   * on. The window is wide in both directions - a client catching up submits
   * hashes for frames it has just replayed, and those are legitimate even
   * though the collector will prune them shortly - and the check is skipped
   * entirely before the room has a tick state, when there is nothing to judge
   * against.
   */
  addStateHash(roomId: string, clientId: string, frame: number, hash: number): void {
    const current = getCurrentFrame(roomId);
    if (current > 0 && (frame > current + HASH_VOTE_AHEAD || frame < current - HASH_VOTE_BEHIND)) {
      this.rejectedStateHashes++;
      return;
    }
    const collector = this.getPartitionCollector(roomId);
    collector.addStateHash(clientId, frame, hash);
  }

  /**
   * Add PARTITION_DATA from a client for distributed state sync.
   */
  /**
   * The same guard as addStateHash, because it is the same hole.
   *
   * Partition data lands in the same frame collections, keyed by the same
   * client-chosen frame, and creating one prunes everything behind it - so the
   * consensus-clearing attack works just as well through this message as
   * through a state hash, and closing only the one it was found in would have
   * left it open.
   *
   * Worth noting what this is guarding: nothing reads partition data. It is
   * accepted, stored for the life of the frame collection, and never consulted
   * by anything. Refusing impossible frames is the conservative fix; not
   * accepting it at all would remove the surface rather than bound it.
   */
  addPartitionData(roomId: string, clientId: string, frame: number, partitionId: number, data: Buffer): void {
    const current = getCurrentFrame(roomId);
    if (current > 0 && (frame > current + HASH_VOTE_AHEAD || frame < current - HASH_VOTE_BEHIND)) {
      this.rejectedStateHashes++;
      return;
    }
    const collector = this.getPartitionCollector(roomId);
    collector.addPartitionData(clientId, frame, partitionId, data);
  }

  /**
   * Remove a disconnected client from partition collector (stops their hash from counting).
   */
  removeClientFromCollector(roomId: string, clientId: string): void {
    const collector = this.partitionCollectors.get(roomId);
    if (collector) {
      collector.removeClient(clientId);
    }
  }


  /**
   * Get majority hash for a frame (for debugging/monitoring).
   */
  getMajorityHash(roomId: string, frame: number): number | null {
    const collector = this.partitionCollectors.get(roomId);
    if (!collector) return null;
    // Pass how many clients are in the room, so a majority means a majority of
    // the room rather than of whichever hashes have arrived so far.
    const room = this.rooms.get(roomId);
    return collector.getMajorityHash(frame, room?.clients.size ?? 0) ?? null;
  }

  /**
   * Get partition collection status for debugging.
   */
  getPartitionStatus(roomId: string, frame: number): {
    exists: boolean;
    hashCount: number;
    partitionCount: number;
    trustedClientCount: number;
    majorityHash: number | null;
    isComplete: boolean;
  } | null {
    const collector = this.partitionCollectors.get(roomId);
    if (!collector) return null;
    return collector.getFrameStatus(frame);
  }

  /**
   * Clean up partition collector when room is deleted.
   */
  private cleanupPartitionCollector(roomId: string): void {
    this.partitionCollectors.delete(roomId);
  }

}

export const roomManager = new RoomManager();
