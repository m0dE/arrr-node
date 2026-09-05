import { roomManager } from './room-manager';
import { queueInputForClients, queueInputForPeers, setCurrentFrame } from './input-batcher';
import { encodeTick, encodeSnapshotUpdate } from './binary-protocol';
import { MessageType } from './types';
import { syncMasterClientList } from './sync-utils';
import { generateInputId } from './utils';
import type { PeerManager } from './peer-manager';

/**
 * What the authority does when a client joins or leaves on another node.
 *
 * Shared because it was written twice and the copies drifted, which is not a
 * tidiness complaint - it was a bug with teeth. Peer messages are dispatched in
 * two places depending on which node dialled which, and only one copy generated
 * the join and leave inputs. The other updated the master client list and
 * stopped there.
 *
 * A join input is how every client learns that a player exists and which
 * connection belongs to them. Without one, that player is not in anybody's
 * roster, and since inputs are attributed by connection and no longer carry a
 * player id to fall back on, everything they do is dropped by everyone. They
 * are connected, playing, and invisible - and whether that happened came down
 * to which node happened to open the peer link.
 *
 * The same applied in reverse on leave: no leave input, so a player who quit
 * stayed in every other client's world forever.
 *
 * This is the third time a duplicated path has produced a fault of this shape
 * in this codebase - three catch-up filters, two resync paths, two peer
 * dispatchers - so the fix is one implementation with two callers rather than
 * two implementations that agree today.
 */

export function applyRelayClientJoin(
  payload: { roomId: string; clientId: string; nodeId: string; isMuted: boolean; metadata?: any },
  peerManager: PeerManager,
): void {
  const { roomId, clientId, nodeId, isMuted, metadata } = payload;
  const room = roomManager.getRoom(roomId);
  if (!room || !room.isAuthority) return;

  roomManager.addToMasterClientList(roomId, clientId, nodeId, isMuted, metadata);

  // Into the replicated stream, so every client sees the join at the same tick
  // and learns who owns the connection it arrived on.
  const joinInput: any = {
    id: generateInputId(),
    clientId,
    type: 'join',
    data: { type: 'join', clientId, user: metadata },
    seq: 0,
    frame: 0,
  };
  const seq = roomManager.addInput(roomId, joinInput);
  if (seq) {
    joinInput.seq = seq;
    queueInputForPeers(roomId, joinInput, peerManager);
    queueInputForClients(roomId, joinInput, peerManager);
  }

  syncMasterClientList(roomId, room, peerManager);
}

/**
 * @param permanent the client actually left, rather than its socket closing.
 *
 * Both cases send this message - the replica has to drop the client from the
 * master list either way - and only one of them ends the player's membership.
 * Treating every closed socket as a departure would delete a player who is
 * three seconds from reconnecting, which is the whole reason `disconnect` and
 * `leave` are different events. So the leave input is generated only for a real
 * departure, and a dropped socket is left to the disconnect input the replica
 * relays separately and to the ghost sweep behind it.
 */
export function applyRelayClientLeave(
  payload: { roomId: string; clientId: string; permanent?: boolean; user?: any },
  peerManager: PeerManager,
): void {
  const { roomId, clientId, permanent, user } = payload;
  const room = roomManager.getRoom(roomId);
  if (!room || !room.isAuthority) return;

  if (permanent) {
    /**
     * Who left, not just which socket closed.
     *
     * This input used to carry the clientId alone. An application keys its
     * world on the player id it was given at join time - the id in `user` -
     * and has no way to turn a connection id into one, so every client simply
     * dropped the event: the player stayed in the world, stayed on the
     * leaderboard, and stayed a target, permanently. The id is taken from the
     * payload where the departing node sent one and from the master client
     * list otherwise, which is the same metadata the join input carried.
     */
    const metadata = user || room.masterClientList?.get(clientId)?.metadata;
    const leaveInput: any = {
      id: generateInputId(),
      clientId,
      type: 'leave',
      data: { type: 'leave', clientId, user: metadata },
      seq: 0,
      frame: 0,
    };
    const seq = roomManager.addInput(roomId, leaveInput);
    if (seq) {
      leaveInput.seq = seq;
      queueInputForPeers(roomId, leaveInput, peerManager);
      queueInputForClients(roomId, leaveInput, peerManager);
    }
  }

  roomManager.removeFromMasterClientList(roomId, clientId);
  syncMasterClientList(roomId, room, peerManager);
}

/**
 * A tick broadcast by the authority, delivered to this node's own clients.
 *
 * The third case that existed twice, and the copies differed in two ways that
 * matter. One wrapped the send to each client in a try/catch and the other did
 * not, so on that link direction a single bad socket threw and every client
 * after it in the list silently missed the tick - a room going quiet for some
 * players and not others, depending on which node opened the link. And the two
 * disagreed about whether to set the current frame for a room this node is not
 * hosting yet.
 *
 * The forgiving send is kept, because losing one client's tick is not a reason
 * to lose everybody's. Setting the frame unconditionally is kept too: the
 * branch below buffers history for a room that has not arrived yet, and that
 * room needs the frame to be right when it does.
 */
export function applyBroadcastInputs(
  payload: { roomId: string; frame: number; events?: any[]; inputs?: any[] },
  peerManager: PeerManager,
): void {
  const { roomId, events, inputs } = payload;
  const frame = payload.frame || 0;
  const inputList = inputs || events || [];
  const room = roomManager.getRoom(roomId);

  // The frame is tracked even for a room this node does not have yet.
  //
  // That looks wasteful - setCurrentFrame creates tick state for whatever id it
  // is handed, and state for a room that never arrives here is never cleaned up
  // by the room's own deletion. I moved this inside the guard below for exactly
  // that reason and it broke a client joining a two-node room: between the room
  // being created here and the next broadcast arriving, getCurrentFrame
  // answered 0, and a client that joined in that window was told to replay to
  // frame 0 and disagreed from the frame it joined on.
  //
  // So the frame stays, and the state it leaves behind is swept up instead -
  // see dropOrphanedTickStates. A cheap sweep is the right price for not having
  // a window where this node cannot say what frame a room is on.
  setCurrentFrame(roomId, frame);

  if (!room) {
    // Not here yet. Hold the history so it is not lost between the room being
    // announced and the room arriving.
    inputList.forEach((input: any) => {
      input.frame = frame;
      roomManager.bufferPeerHistory(roomId, input);
    });
    return;
  }

  // Being sent a broadcast settles who the authority is: not this node.
  if (room.isAuthority) roomManager.setAuthority(roomId, false);

  inputList.forEach((input: any) => {
    input.frame = frame;
    roomManager.storeInput(roomId, input);
  });

  // Carries the consensus this node has the votes to compute, so clients here
  // can tell whether they still agree with the room.
  const relayMajority = roomManager.getMajorityHash(roomId, frame - 1) ?? undefined;
  const tickMessage = encodeTick(frame, inputList, undefined, undefined, relayMajority);

  if (process.env.TICK_TRACE && inputList.length) {
    console.log(`[FWD-TRACE] room=${roomId} frame=${frame} seqs=${inputList.map((i: any) => i.seq).join(',')}`
      + ` -> ${roomManager.getClients(roomId).length} local clients`);
  }

  for (const client of roomManager.getClients(roomId)) {
    try {
      client.socket.send(tickMessage);
    } catch (err) {
      console.error(`Failed to forward tick to client ${client.id}:`, err);
    }
  }
}

/**
 * A room replicated to this node by the authority.
 *
 * The fourth case that existed twice. The copy that ran on a dialled link
 * created the room and stopped; the other also carried across snapshotFrame and
 * whether the snapshot's seq was a server guess rather than a client's claim.
 *
 * Those two fields are not decoration - selectCatchUpInputs reads them to
 * decide whether to filter a catch-up by frame or by sequence. A replica
 * created without them filters by the wrong rule, so anybody joining on that
 * node gets the wrong slice of history and replays into a world nobody else is
 * in. Which rule applied came down to which node opened the peer link.
 */
export function applyReplicateRoom(
  payload: { room: any },
  peerManager: PeerManager,
): void {
  const { room } = payload;
  const inputs = room.inputs || room.events || [];
  const existingRoom = roomManager.getRoom(room.id);

  if (!existingRoom) {
    const newRoom = roomManager.createRoom(room.id, room.snapshot, inputs, false);
    if (newRoom) {
      // createRoom stores the snapshot directly rather than going through
      // updateSnapshot, so carry the origin's verdict across by hand -
      // otherwise a replicated guess reads as a client's claim here.
      newRoom.snapshotFrame = room.snapshot?.frame || 0;
      newRoom.snapshotSeqInferred = room.snapshotSeqInferred === true;
    }
    if (newRoom && room.masterClientList) {
      const masterList = new Map();
      room.masterClientList.forEach((c: any) => masterList.set(c.clientId, c));
      newRoom.masterClientList = masterList;
    }
    console.log(`Room ${room.id} replicated from peer with ${inputs.length} inputs`);

    // Anything that arrived before the room did still has to reach the
    // authority.
    const pendingInputs = roomManager.consumePendingRelayInputs(room.id);
    for (const input of pendingInputs) {
      peerManager.broadcastToPeers({
        type: MessageType.RELAY_INPUT,
        payload: { roomId: room.id, input },
      });
    }
  } else if (existingRoom.isAuthority) {
    // Both nodes believed they were authority. Defer to the other one, which
    // most likely created the room first, and take its history.
    console.log(`Room ${room.id} conflict - deferring authority to peer`);
    roomManager.setAuthority(room.id, false);
    inputs.forEach((input: any) => roomManager.storeInput(room.id, input));
  }
}

/**
 * Room state offered by a peer: history to merge, and possibly a newer snapshot.
 *
 * The fifth, and the one that shows most clearly why sharing these matters. A
 * bug here was found and fixed once: input history was being merged inside the
 * "is the snapshot newer" branch, so a peer offering state while its snapshot
 * was not newer - the normal case just after a link comes up, when both sides
 * still have an empty snapshot - had its history dropped on the floor. The
 * receiving node never learned about joins from before it connected and its
 * clients disagreed about who was in the room for the rest of the session.
 *
 * That fix was applied to one copy. The other still merged inside the branch,
 * so the bug was live on links opened the other way round the whole time.
 *
 * A snapshot and the history are independent: being handed an older snapshot
 * says nothing about whether the history alongside it contains inputs we are
 * missing. So the merge happens first, and unconditionally.
 */
export function applySyncRoomState(payload: { rooms: any[] }): void {
  for (const room of payload.rooms || []) {
    const inputs = room.inputs || room.events || [];
    const existingRoom = roomManager.getRoom(room.id);

    if (!existingRoom) {
      const newRoom = roomManager.createRoom(room.id, room.snapshot, inputs, false);
      if (newRoom) {
        // Carry these across, exactly as the replicate path does.
        //
        // createRoom stores the snapshot directly instead of going through
        // updateSnapshot, so without this the room is left with snapshotFrame 0
        // and no verdict on whether the snapshot's seq was a client's claim or
        // a server's guess. selectCatchUpInputs reads both to choose between
        // filtering a catch-up by frame and filtering it by sequence, so a room
        // created here handed every late joiner a slice of history chosen by
        // the wrong rule.
        //
        // Found by running the demos across two nodes for the first time: one
        // client per room diverged on the very frame it joined and then
        // resynced every hundred frames for the rest of the session. Delivery
        // to it was clean throughout - same ticks as everybody else - because
        // what was wrong arrived before any of them, in the history it was
        // given to start from.
        newRoom.snapshotFrame = room.snapshot?.frame || 0;
        newRoom.snapshotSeqInferred = room.snapshotSeqInferred === true;
      }
      if (newRoom && room.masterClientList) {
        const masterList = new Map();
        room.masterClientList.forEach((c: any) => masterList.set(c.clientId, c));
        newRoom.masterClientList = masterList;
      }
      console.log(`Room ${room.id} synced from peer with ${inputs.length} inputs`);
      continue;
    }

    if (inputs.length > 0) {
      const existingSeqs = new Set(existingRoom.inputs.map((e: any) => e.seq));
      const newInputs = inputs.filter((e: any) => !existingSeqs.has(e.seq));
      if (newInputs.length > 0) {
        existingRoom.inputs.push(...newInputs);
        existingRoom.inputs.sort((a: any, b: any) => (a.seq || 0) - (b.seq || 0));
        console.log(`[SYNC] Room ${room.id} merged ${newInputs.length} inputs from peer`);
      }
    }

    if (!room.snapshot) continue;

    // Accept on frame where there is one. An inferred seq is a lower bound and
    // can legitimately be 0, so comparing seq alone rejects genuinely newer
    // snapshots and leaves a replica stuck on stale state indefinitely.
    const snapshotSeq = room.snapshot.seq || 0;
    const currentSeq = existingRoom.snapshot?.seq || 0;
    const incomingFrame = room.snapshot?.frame || 0;
    const currentFrame = existingRoom.snapshot?.frame || 0;
    const isNewer = (incomingFrame > 0 || currentFrame > 0)
      ? incomingFrame > currentFrame
      : snapshotSeq > currentSeq;
    if (!isNewer) continue;

    const hash = room.snapshotHash || roomManager.computeHash(room.snapshot);
    // Carry the origin's verdict on whether snapshot.seq is a client's claim or
    // a server guess, so a guess is not laundered into a claim here.
    roomManager.updateSnapshot(room.id, room.snapshot, hash, { seqInferred: room.snapshotSeqInferred === true });

    if (!existingRoom.isAuthority && room.masterClientList) {
      const masterList = new Map();
      room.masterClientList.forEach((c: any) => masterList.set(c.clientId, c));
      existingRoom.masterClientList = masterList;
    }

    // Forward to local clients so they can correct drift.
    const clients = roomManager.getClients(room.id);
    if (clients.length > 0) {
      const snapshotMsg = encodeSnapshotUpdate(room.id, room.snapshot, hash);
      for (const client of clients) {
        try {
          client.socket.send(snapshotMsg);
        } catch (err) {
          console.error(`Failed to forward snapshot to client ${client.id}:`, err);
        }
      }
    }
  }
}
