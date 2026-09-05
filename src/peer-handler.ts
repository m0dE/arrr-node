import { Message, MessageType, Room } from './types';
import { roomManager } from './room-manager';
import { voiceRelay } from './voice-relay';
import type { PeerManager } from './peer-manager';
import { noteHashRelayReceived } from './peer-manager';
import { queueInputForClients, queueInputForPeers, setCurrentFrame } from './input-batcher';
import { syncMasterClientList } from './sync-utils';
import { encodeTick, encodeSnapshotUpdate, encodeClientListUpdate } from './binary-protocol';
import type WebSocket from 'ws';
import { generateInputId } from './utils';
import { verifyMeshToken, meshAuthMode } from './mesh-auth';
import {
  applyRelayClientJoin, applyRelayClientLeave, applyBroadcastInputs,
  applyReplicateRoom, applySyncRoomState,
} from './peer-membership';

export function handlePeerConnection(socket: WebSocket, peerManager: PeerManager, _nodeId: string) {
  let peerId: string | null = null;

  /**
   * Whether this socket has presented a credential central minted for us.
   *
   * A flag on the connection rather than a check inside the identify branch,
   * because the identify branch is not the only way in. PEER_IDENTIFY_ACK also
   * calls registerIncomingPeer, so checking only the first of them leaves the
   * second as an unguarded door into exactly the same place - which is the
   * shape of the hole this whole change exists to close, reintroduced by the
   * fix for it. Everything below the handshake is gated on this instead.
   */
  let authenticated = false;

  console.log(`Peer connection opened`);

  /** True when this socket may not do what it just asked to do. */
  const unauthenticated = (what: string): boolean => {
    if (authenticated) return false;
    if (meshAuthMode === 'warn') {
      console.warn(
        `[MESH-AUTH] PERMITTING ${what} from an unauthenticated peer because ` +
          `MESH_AUTH_MODE=warn. This would be refused in enforce mode.`
      );
      return false;
    }
    console.warn(`[MESH-AUTH] Refused ${what} from an unauthenticated peer`);
    try { socket.close(); } catch { /* already gone */ }
    return true;
  };

  socket.on('message', (data: Buffer) => {
    try {
      const message: Message = JSON.parse(data.toString());

      // Handle peer identification
      if (message.type === MessageType.PEER_IDENTIFY) {
        peerId = message.payload.nodeId;
        const peerUrl = message.payload.url;

        // Becoming a peer is what authorises everything below - RELAY_INPUT in
        // particular, which hands the authority an input carrying whatever
        // clientId the sender wrote. The socket itself is unauthenticated, so
        // the handshake is where the check has to be, and it has to happen
        // before registerIncomingPeer: a peer that is registered and then
        // rejected is still in the map, and everything this node broadcasts to
        // peers goes to it.
        const auth = verifyMeshToken(message.payload.token, 'peer');
        if (auth.ok) {
          authenticated = true;
        } else if (meshAuthMode === 'warn') {
          console.warn(
            `[MESH-AUTH] PERMITTING unauthenticated peer ${peerId} (${auth.reason}) because ` +
              `MESH_AUTH_MODE=warn. This peer would be refused in enforce mode.`
          );
          // Marked authenticated so the per-message gate below does not say the
          // same thing again for every input this peer relays. One line per
          // connection is the useful volume; one per message buries it.
          authenticated = true;
        } else {
          console.warn(`[MESH-AUTH] Refused peer ${peerId}: ${auth.reason}`);
          try { socket.close(); } catch { /* already gone */ }
          return;
        }

        console.log(`Peer identified: ${peerId} with URL: ${peerUrl}`);

        // Register this as an incoming peer connection
        if (peerId && peerUrl) {
          peerManager.registerIncomingPeer(peerId, peerUrl, socket);
        }

        // Send acknowledgment back (use registered ID and URL from peer manager)
        socket.send(JSON.stringify({
          type: MessageType.PEER_IDENTIFY_ACK,
          payload: {
            nodeId: peerManager.getNodeId(),
            url: peerManager.getNodeUrl()
          }
        }));
        return;
      }

      // Handle peer identification acknowledgment
      if (message.type === MessageType.PEER_IDENTIFY_ACK) {
        // An acknowledgment registers a peer just as an identify does, so it is
        // the same door and needs the same lock. It cannot carry its own
        // credential - the ack is sent by whoever accepted the link, and has
        // never carried one - so what it requires is that this socket already
        // completed the handshake.
        if (unauthenticated('PEER_IDENTIFY_ACK')) return;

        peerId = message.payload.nodeId;
        const peerUrl = message.payload.url;
        console.log(`Peer identification acknowledged: ${peerId} with URL: ${peerUrl}`);

        // Register this as an incoming peer connection
        if (peerId && peerUrl) {
          peerManager.registerIncomingPeer(peerId, peerUrl, socket);
        }
        // Don't send anything back - this breaks the loop
        return;
      }

      // Everything past the handshake acts on a room: relaying an input for
      // sequencing, voting on a state hash, replacing a room's state wholesale.
      // None of it is available until this socket has proved it is a peer
      // central told us about.
      if (unauthenticated(String(message.type))) return;

      switch (message.type) {
        case MessageType.RELAY_VOICE: {
          // Both dispatch paths again, for the reason given under
          // RELAY_STATE_HASH below: a link this node accepted lands here.
          voiceRelay.onRelayed(message.payload);
          break;
        }

        case MessageType.RELAY_STATE_HASH: {
          // A client on another node voted on a frame. Folded in here so the
          // majority is a property of the room rather than of whichever node a
          // client happened to be routed to.
          //
          // This case exists twice, because peer messages are dispatched in two
          // places: this switch handles a link this node accepted, and
          // peerManager.handlePeerMessage handles one this node dialled. Adding
          // it to only one of them made the relay work in exactly one direction
          // - measured as one node receiving 1,440 hashes and the other zero,
          // which reads like a broken link and is really a missing case.
          //
          // The counter had the same split and was fixed a step later, so the
          // health endpoint went on reporting exactly the symptom of the bug
          // that had already been fixed: after two hours of live play one node
          // showed 772,395 relays sent and its peer showed 0 received, while
          // every one of those hashes was in fact being folded in right here.
          // A counter that only counts half the paths is worse than no counter,
          // because it is believed.
          noteHashRelayReceived();
          const { roomId, clientId, frame, stateHash } = message.payload || {};
          if (roomId && clientId && typeof frame === 'number' && typeof stateHash === 'number') {
            roomManager.addStateHash(roomId, clientId, frame, stateHash);
          }
          break;
        }

        case MessageType.SYNC_ROOM_STATE: {
          // Shared; see peer-membership.ts. The two copies of this differed on
          // whether input history is merged when the incoming snapshot is not
          // newer, which is a bug that was fixed in one of them only.
          applySyncRoomState(message.payload);
          break;
        }

        case MessageType.RELAY_EVENT:
        case MessageType.RELAY_INPUT: {
          const { roomId, event, input: payloadInput } = message.payload;
          const relayInput = payloadInput || event; // Support both names
          const room = roomManager.getRoom(roomId);

          const inputType = relayInput?.type || relayInput?.data?.type || 'unknown';
          console.log(`[PEER] Received RELAY_INPUT (${inputType}) for room ${roomId}, roomExists=${!!room}, isAuthority=${room?.isAuthority}, peerId=${peerId}`);

          if (room && room.isAuthority) {
            // Skip inputs that already have sequence numbers (prevents duplicates from buffered relay)
            if (relayInput.seq !== undefined && relayInput.seq > 0) {
              break;
            }
            const seq = roomManager.addInput(roomId, relayInput);

            if (seq) {
              // Queue the very object that went into room.inputs, not a copy.
              //
              // sendTick stamps input.frame on whatever it broadcasts. Queuing a
              // copy meant only the copy got a frame, and the history entry kept
              // frame === undefined forever - which every late-joiner filter
              // drops on sight. The effect was that a late joiner silently never
              // replayed a single input that originated on a replica node.
              relayInput.seq = seq;
              queueInputForPeers(roomId, relayInput, peerManager);
              queueInputForClients(roomId, relayInput, peerManager);
            }
          }
          break;
        }

        case MessageType.BROADCAST_EVENTS:
        case MessageType.BROADCAST_INPUTS: {
          // Shared with the other peer dispatcher; see peer-membership.ts. The
          // two copies of this disagreed about whether a failing client socket
          // should stop the tick reaching the clients after it.
          applyBroadcastInputs(message.payload, peerManager);
          break;
        }

        case MessageType.REPLICATE_ROOM: {
          // Shared; see peer-membership.ts. One copy did not carry across the
          // fields that decide how a catch-up is filtered.
          applyReplicateRoom(message.payload, peerManager);
          break;
        }

        case MessageType.RELAY_CLIENT_JOIN: {
          // Shared with the dialled-link dispatcher in peer-manager. These were
          // two implementations once and only this one generated the join
          // input; see peer-membership.ts.
          applyRelayClientJoin(message.payload, peerManager);
          break;
        }

        case MessageType.RELAY_CLIENT_LEAVE: {
          applyRelayClientLeave(message.payload, peerManager);
          break;
        }

        case MessageType.SYNC_MASTER_CLIENT_LIST: {
          // Authority → Replica: Sync full master client list
          const { roomId, clients } = message.payload;
          console.log(`[MASTER_CLIENT_LIST] Received SYNC_MASTER_CLIENT_LIST for room ${roomId}: ${clients.length} clients`);

          const room = roomManager.getRoom(roomId);
          if (!room) {
            console.log(`[MASTER_CLIENT_LIST] Room ${roomId} not found locally`);
            break;
          }

          if (room.isAuthority) {
            console.log(`[MASTER_CLIENT_LIST] We are authority, ignoring sync`);
            break;
          }

          // Update our local copy of the master client list
          const masterList = new Map();
          for (const client of clients) {
            masterList.set(client.clientId, client);

            // Update local client if it exists on this node
            const localClient = room.clients.get(client.clientId);
            if (localClient && localClient.isMuted !== client.isMuted) {
              console.log(`[MASTER_CLIENT_LIST] Updating local client ${client.clientId} isMuted: ${localClient.isMuted} -> ${client.isMuted}`);
              localClient.isMuted = client.isMuted;
            }
          }
          room.masterClientList = masterList;

          console.log(`[MASTER_CLIENT_LIST] Updated master list for room ${roomId}: ${masterList.size} clients`);

          // Broadcast CLIENT_LIST_UPDATE to all our local clients
          const clientListArray = Array.from(masterList.values()).map(c => ({
            clientId: c.clientId,
            metadata: c.metadata,
            isMuted: c.isMuted
          }));

          const updateMessage = encodeClientListUpdate(roomId, clientListArray);

          for (const [_clientId, client] of room.clients) {
            if (client.socket.readyState === 1) { // WebSocket.OPEN
              client.socket.send(updateMessage);
            }
          }

          console.log(`[MASTER_CLIENT_LIST] Broadcasted CLIENT_LIST_UPDATE to ${room.clients.size} local clients`);
          break;
        }

      }
    } catch (error) {
      console.error('Error handling peer message:', error);
    }
  });

  socket.on('close', () => {
    if (peerId) {
      peerManager.handlePeerDisconnect(peerId);
    }
    console.log(`Peer disconnected: ${peerId || 'unknown'}`);
  });

  socket.on('error', (error: Error) => {
    console.error(`Peer error:`, error.message);
  });
}
