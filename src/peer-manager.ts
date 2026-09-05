import WebSocket from 'ws';
import { PeerNode, Message, MessageType } from './types';
import { peerUrlFor } from './utils';
import { roomManager } from './room-manager';
import { voiceRelay } from './voice-relay';
import { encodeTick, encodeSnapshotUpdate, encodeClientListUpdate } from './binary-protocol';
import { syncMasterClientList } from './sync-utils';
import { queueInputForClients, queueInputForPeers, setCurrentFrame } from './input-batcher';
import {
  applyRelayClientJoin, applyRelayClientLeave, applyBroadcastInputs,
  applyReplicateRoom, applySyncRoomState,
} from './peer-membership';

class PeerManager {
  private peers: Map<string, PeerNode> = new Map();
  private pendingConnections: Map<string, Promise<boolean>> = new Map();
  private nodeId: string;
  private nodeUrl: string = '';

  constructor(nodeId: string) {
    this.nodeId = nodeId;
  }

  // Update node ID after registration with central service
  // Counted so the direction of peer traffic can be seen rather than inferred.
  // Relayed hashes appeared to flow one way only, and the vote totals alone
  // could not say whether that was a send that never happened or a receive
  // that was dropped.
  static hashRelaySent = 0;
  static hashRelayReceived = 0;
  static hashRelayNoPeers = 0;

  setNodeId(nodeId: string): void {
    this.nodeId = nodeId;
  }

  // Set our node's connection URL
  setNodeUrl(url: string): void {
    this.nodeUrl = url;
  }

  getNodeId(): string {
    return this.nodeId;
  }

  getNodeUrl(): string {
    return this.nodeUrl;
  }

  // Convert client URL to peer URL
  private getPeerUrl(clientUrl: string): string {
    return peerUrlFor(clientUrl);
  }

  /**
   * Dial a peer.
   *
   * `peerToken` is a credential central minted for the node being dialled, not
   * for this one, and it is presented unaltered in the identify handshake. A
   * dial without one is refused by an up-to-date peer unless it is running in
   * MESH_AUTH_MODE=warn, so every path that dials has to have obtained one.
   */
  async connectToPeer(peerId: string, clientUrl: string, peerToken?: string): Promise<boolean> {
    if (this.isSelf(peerId, 'dial')) return false;

    // Already connected. Note we check isConnected, not just presence: a dropped
    // peer keeps its map entry (with isConnected=false) so that in-flight lookups
    // still resolve, and short-circuiting on presence alone would mean a peer that
    // disconnects once is never redialled.
    const existing = this.peers.get(peerId);
    if (existing?.isConnected) {
      return true;
    }
    if (existing) {
      // Stale entry from a previous session - drop it so we dial fresh.
      try {
        existing.socket?.close();
      } catch {
        // Already closed
      }
      this.peers.delete(peerId);
      console.log(`Reconnecting to previously disconnected peer: ${peerId}`);
    }

    // Connection already in progress - wait for it
    const pending = this.pendingConnections.get(peerId);
    if (pending) {
      return pending;
    }

    const peerUrl = this.getPeerUrl(clientUrl);

    const connectionPromise = new Promise<boolean>((resolve) => {
      try {
        const ws = new WebSocket(peerUrl);
        let resolved = false;

        ws.on('open', () => {
          // Send identification with our connection URL
          ws.send(JSON.stringify({
            type: MessageType.PEER_IDENTIFY,
            payload: {
              nodeId: this.nodeId,
              url: this.nodeUrl,
              token: peerToken
            }
          }));

          // Add peer to map immediately so messages can be sent
          // But wait for PEER_IDENTIFY_ACK before resolving
          this.peers.set(peerId, {
            id: peerId,
            url: peerUrl,
            socket: ws,
            isConnected: true
          });
        });

        ws.on('message', (data: Buffer) => {
          try {
            const message: Message = JSON.parse(data.toString());

            // Wait for PEER_IDENTIFY_ACK before considering connection fully established
            if (message.type === MessageType.PEER_IDENTIFY_ACK && !resolved) {
              resolved = true;
              // Sync room states with peer after handshake is complete
              this.syncRoomStates(peerId);
              resolve(true);
              return;
            }
          } catch (e) {
            // Ignore parse errors, let handlePeerMessage handle them
          }

          this.handlePeerMessage(data);
        });

        ws.on('close', () => {
          console.log(`Peer disconnected: ${peerId}`);
          const peer = this.peers.get(peerId);
          if (peer) {
            peer.isConnected = false;
          }
          if (!resolved) {
            resolved = true;
            resolve(false);
          }
        });

        ws.on('error', (error) => {
          console.error(`Peer connection error for ${peerId}:`, error.message);
          if (!resolved) {
            resolved = true;
            resolve(false);
          }
        });

        // Timeout after 5 seconds
        setTimeout(() => {
          if (!resolved) {
            console.error(`Peer connection timeout for ${peerId}`);
            resolved = true;
            this.pendingConnections.delete(peerId);
            resolve(false);
          }
        }, 5000);
      } catch (error) {
        console.error(`Failed to connect to peer ${peerId}:`, error);
        this.pendingConnections.delete(peerId);
        resolve(false);
      }
    });

    this.pendingConnections.set(peerId, connectionPromise);

    connectionPromise.finally(() => {
      this.pendingConnections.delete(peerId);
    });

    return connectionPromise;
  }

  /**
   * A node is not its own peer.
   *
   * Checked at the two doors into the peer map rather than at the callers,
   * because the callers are the part that keeps getting this wrong. Both
   * join-time dials read the authority out of central's answer and dial it
   * without asking whether it is this node, and the authority they read can
   * name this node: it changes between the enclosing "am I the authority"
   * test and the dial, which is precisely what a central restart produces.
   *
   * Measured: node1 logged `Connecting to authority peer node_...1788d47ec36f`
   * where node_...1788d47ec36f is node1, dialled ws://localhost:8301/ws - its
   * own address - and its own identify came back through its own /ws/peer.
   *
   * The consequence is not a wasted socket. Everything this node broadcasts to
   * its peers is then delivered to itself, and handleBroadcastInputs treats
   * being sent a broadcast as settling who the authority is - not this node -
   * so the node demotes itself on its own tick. Central still names it the
   * authority, the replica sweep promotes it back, and it demotes again on the
   * next broadcast. While it flaps, every join arriving here is relayed to
   * "the authority" over the peer link instead of being sequenced locally, and
   * the peer that receives it does not have the room. The joins are dropped and
   * the room's roster stays at whoever created it, for the rest of its life,
   * while the world ticks on and every hash agrees.
   *
   * That is what three specs were failing on: `p2 never saw all 5 players join
   * (saw 1: p1)`, with the creator at frame 624 and a roster of 1.
   */
  private isSelf(peerId: string, what: string): boolean {
    if (peerId !== this.nodeId) return false;
    console.warn(`[PEER] Refusing to ${what} this node as its own peer (${peerId}). `
      + 'A self-link makes every broadcast return to the sender, which reads as '
      + 'another node holding authority.');
    return true;
  }

  // Register incoming peer connection (from peer-handler)
  registerIncomingPeer(peerId: string, peerUrl: string, socket: WebSocket): void {
    if (this.isSelf(peerId, 'register')) {
      try { socket.close(); } catch { /* already gone */ }
      return;
    }
    const existing = this.peers.get(peerId);
    if (existing && existing.isConnected) {
      // Already have an outgoing connection, update socket for bidirectional
      // but don't count as new peer.
      console.log(`Peer ${peerId} already connected (outgoing), updating incoming socket`);
      // Still sync room state. A peer re-identifying is exactly what happens when
      // it joins a room it has not seen before on a link that is already up, and
      // returning early here meant it was never told about that room - so it
      // learned only from the incremental broadcasts that followed and missed
      // everything before them. Sync is idempotent: the receiver merges by seq.
      this.syncRoomStates(peerId);
      return;
    }

    this.peers.set(peerId, {
      id: peerId,
      url: peerUrl,
      socket,
      isConnected: true
    });

    console.log(`Registered incoming peer: ${peerId} with URL: ${peerUrl}`);

    // Sync room states with the new peer
    this.syncRoomStates(peerId);
  }

  handlePeerDisconnect(peerId: string): void {
    const peer = this.peers.get(peerId);
    if (peer) {
      peer.isConnected = false;

      // Notify room manager that this node is gone
      // Authority node will remove all clients associated with this nodeId
      const rooms = roomManager.getAllRooms();
      rooms.forEach(room => {
        if (room.isAuthority) {
          const removed = roomManager.removeClientsFromNode(room.id, peerId);
          if (removed.length > 0) {
            console.log(`[MASTER_CLIENT_LIST] Removed ${removed.length} clients from disconnected peer ${peerId} in room ${room.id}`);
            syncMasterClientList(room.id, room, this);
          }
        }
      });
    }
  }

  // Handle messages received on outgoing peer connections
  handlePeerMessage(data: Buffer): void {
    try {
      const message: Message = JSON.parse(data.toString());

      // Handle peer identify acknowledgments
      if (message.type === MessageType.PEER_IDENTIFY_ACK) {
        console.log(`Received PEER_IDENTIFY_ACK from peer: ${message.payload.nodeId}`);
        return;
      }

      switch (message.type) {
        case MessageType.RELAY_VOICE: {
          // Somebody on another node said something. Fanned out here to this
          // node's own listeners by distance, exactly as a local packet is.
          voiceRelay.onRelayed(message.payload);
          break;
        }

        case MessageType.RELAY_STATE_HASH: {
          PeerManager.hashRelayReceived++;
          // Another node's client voted on a frame. Folding it in here is what
          // makes the majority a property of the room instead of a property of
          // whichever node a client happened to be routed to.
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
          const relayInput = payloadInput || event;
          const room = roomManager.getRoom(roomId);

          if (room && room.isAuthority) {
            if (relayInput.seq !== undefined && relayInput.seq > 0) {
              break;
            }
            const seq = roomManager.addInput(roomId, relayInput);
            if (seq) {
              // Queue the stored object, not a copy - see the identical fix in
              // peer-handler.ts and index.ts. Broadcasting a copy leaves the
              // history entry with frame === undefined, which every catch-up
              // filter reads as "not broadcast yet" and skips forever.
              relayInput.seq = seq;
              queueInputForPeers(roomId, relayInput, this);
              queueInputForClients(roomId, relayInput, this);
            }
          }
          break;
        }

        case MessageType.BROADCAST_EVENTS:
        case MessageType.BROADCAST_INPUTS: {
          // Shared with the other peer dispatcher; see peer-membership.ts. The
          // two copies of this disagreed about whether a failing client socket
          // should stop the tick reaching the clients after it.
          applyBroadcastInputs(message.payload, this);
          break;
        }

        case MessageType.REPLICATE_ROOM: {
          // Shared; see peer-membership.ts. One copy did not carry across the
          // fields that decide how a catch-up is filtered.
          applyReplicateRoom(message.payload, this);
          break;
        }

        case MessageType.RELAY_CLIENT_JOIN: {
          // Shared with the accepted-link dispatcher in peer-handler. This copy
          // used to update the master client list and stop there, so whether a
          // joining player was ever announced to the room depended on which
          // node opened the peer link; see peer-membership.ts.
          applyRelayClientJoin(message.payload, this);
          break;
        }

        case MessageType.RELAY_CLIENT_LEAVE: {
          applyRelayClientLeave(message.payload, this);
          break;
        }

        case MessageType.SYNC_MASTER_CLIENT_LIST: {
          const { roomId, clients } = message.payload;
          const room = roomManager.getRoom(roomId);
          if (room && !room.isAuthority) {
            const masterList = new Map();
            for (const client of clients) {
              masterList.set(client.clientId, client);
              const localClient = room.clients.get(client.clientId);
              if (localClient) localClient.isMuted = client.isMuted;
            }
            room.masterClientList = masterList;

            const clientListArray = Array.from(masterList.values()).map(c => ({
              clientId: c.clientId,
              metadata: c.metadata,
              isMuted: c.isMuted
            }));
            const updateMessage = encodeClientListUpdate(roomId, clientListArray);
            for (const [_clientId, client] of room.clients) {
              if (client.socket.readyState === 1) client.socket.send(updateMessage);
            }
          }
          break;
        }
      }
    } catch (error) {
      console.error('Error handling peer message:', error);
    }
  }

  /**
   * Send this node's authoritative room state to a peer that has just connected.
   *
   * Without this a peer only ever learns about a room from the incremental
   * BROADCAST_INPUTS that follow, so everything that happened before the link
   * came up is invisible to it - and to its clients, permanently. That shows up
   * as two nodes disagreeing about who is in the room.
   *
   * Only authoritative rooms are sent: a replica's copy is by definition behind
   * the authority's, and pushing it back would fight the real owner.
   */
  syncRoomStates(peerId: string): void {
    const peer = this.peers.get(peerId);
    if (!peer || !peer.socket) return;

    const rooms = roomManager.getAllRooms().filter(room => room.isAuthority);
    if (rooms.length === 0) return;

    const roomData = rooms.map(room => ({
      id: room.id,
      snapshot: room.snapshot,
      snapshotHash: room.snapshotHash,
      snapshotTimestamp: room.snapshotTimestamp,
      // Whether snapshot.seq is a client's claim or a server's guess. The
      // receiver cannot tell from the payload alone - a guessed seq is still a
      // number - so it has to travel with the snapshot.
      snapshotSeqInferred: room.snapshotSeqInferred === true,
      inputs: room.inputs,
      events: room.inputs, // Backwards compatibility
      isAuthority: room.isAuthority,
      masterClientList: room.masterClientList ? Array.from(room.masterClientList.values()) : []
    }));

    peer.socket.send(JSON.stringify({
      type: MessageType.SYNC_ROOM_STATE,
      payload: { rooms: roomData }
    }));
  }

  /** Is there anybody to relay to at all. Cheap; asked on every voice packet. */
  hasConnectedPeers(): boolean {
    for (const p of this.peers.values()) if (p.isConnected && p.socket) return true;
    return false;
  }

  sendToPeer(peerId: string, message: Message): boolean {
    const peer = this.peers.get(peerId);
    if (!peer || !peer.isConnected || !peer.socket) return false;
    try {
      peer.socket.send(JSON.stringify(message));
      return true;
    } catch (error) {
      console.error(`Failed to send message to peer ${peerId}:`, error);
      return false;
    }
  }

  broadcastToPeers(message: Message): void {
    const connectedPeers = Array.from(this.peers.values()).filter(p => p.isConnected && p.socket);
    if (message.type === MessageType.RELAY_STATE_HASH) {
      PeerManager.hashRelaySent += connectedPeers.length;
      if (connectedPeers.length === 0) PeerManager.hashRelayNoPeers++;
    }

    // Debug log for relay events
    if (message.type === MessageType.RELAY_EVENT) {
      const eventType = (message.payload as any)?.event?.type || (message.payload as any)?.event?.data?.type || 'unknown';
      console.log(`[PEER] Broadcasting RELAY_EVENT (${eventType}) to ${connectedPeers.length} peers (total peers: ${this.peers.size})`);
    }
    if (message.type === MessageType.BROADCAST_INPUTS) {
      const inputList = (message.payload as any)?.inputs || [];
      const joinInputs = inputList.filter((e: any) => e.data?.type === 'join' || e.type === 'join');
      const disconnectInputs = inputList.filter((e: any) => e.type === 'disconnect' || e.data?.type === 'disconnect');
      if (joinInputs.length > 0) {
        console.log(`[PEER] Broadcasting BROADCAST_INPUTS with ${joinInputs.length} join inputs to ${connectedPeers.length} peers, peerIds: ${connectedPeers.map(p => p.id).join(",")}`);
      }
      if (disconnectInputs.length > 0) {
        console.log(`[PEER] Broadcasting BROADCAST_INPUTS with ${disconnectInputs.length} disconnect inputs to ${connectedPeers.length} peers, peerIds: ${connectedPeers.map(p => p.id).join(",")}`);
      }
    }

    // Serialize once, not once per peer - this runs on every authority tick.
    const payload = JSON.stringify(message);
    connectedPeers.forEach(peer => {
      try {
        peer.socket!.send(payload);
      } catch (error) {
        console.error(`[PEER] Failed to broadcast ${message.type} to peer ${peer.id}:`, error);
      }
    });
  }

  getPeers(): PeerNode[] {
    return Array.from(this.peers.values());
  }

  requestReplication(roomId: string): void {
    const peers = this.getPeers().filter(p => p.isConnected);
    const room = roomManager.getRoom(roomId);
    if (!room) return;

    const roomData = {
      id: room.id,
      snapshot: room.snapshot,
      snapshotHash: room.snapshotHash,
      snapshotTimestamp: room.snapshotTimestamp,
      // See syncRoomStates: a guessed seq is indistinguishable from a claimed
      // one in the payload, so the verdict has to travel alongside it.
      snapshotSeqInferred: room.snapshotSeqInferred === true,
      inputs: room.inputs,
      events: room.inputs, // Backwards compatibility
      masterClientList: room.masterClientList ? Array.from(room.masterClientList.values()) : []
    };

    peers.forEach(peer => {
      if (peer.socket) {
        peer.socket.send(JSON.stringify({
          type: MessageType.REPLICATE_ROOM,
          payload: { room: roomData }
        }));
      }
    });
  }
}

/**
 * Counted through a function because the class is exported as a type only.
 * Both peer dispatch paths need to reach this - the dialled link through
 * handlePeerMessage and the accepted one through peer-handler - and a counter
 * only one of them can touch is how the relay came to look one-directional
 * long after it had been made to work in both.
 */
export function noteHashRelayReceived(): void {
  PeerManager.hashRelayReceived++;
}

/** Counters only, for the health endpoint - not a way to reach the instance. */
export const peerRelayStats = {
  get sent() { return PeerManager.hashRelaySent; },
  get received() { return PeerManager.hashRelayReceived; },
  get noPeers() { return PeerManager.hashRelayNoPeers; },
};

export function createPeerManager(nodeId: string): PeerManager {
  return new PeerManager(nodeId);
}

export type { PeerManager };
