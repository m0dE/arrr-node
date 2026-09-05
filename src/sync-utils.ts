import { MessageType, Room } from './types';
import { PeerManager } from './peer-manager';
import { encodeClientListUpdate } from './binary-protocol';

/**
 * @param opts.peersOnly send only to other nodes, not to this node's clients.
 *
 * Every caller but one is reacting to a change, and a change is worth telling
 * the clients about. The periodic reconciliation is not: it re-sends the same
 * list on a timer so a replica that missed an update is corrected, and pushing
 * that to every local client as well meant each of them received an unsolicited
 * client-list update every fifteen seconds per room.
 *
 * That is not free. The full suite went from 44 passing to 10 failing when the
 * timer was added, with failures spread across membership, joining, prediction
 * and publisher election - all of them shapes where a client acts on the client
 * list. Bisected to the commit, then to this line: the reconciliation is for the
 * replicas, and the clients had already been told.
 */
export function syncMasterClientList(
    roomId: string,
    room: Room,
    peerManager: PeerManager,
    opts: { peersOnly?: boolean } = {},
) {
    if (!room.masterClientList) return;

    const masterListArray = Array.from(room.masterClientList.values());

    // 1. Sync to all replicas
    peerManager.broadcastToPeers({
        type: MessageType.SYNC_MASTER_CLIENT_LIST,
        payload: { roomId, clients: masterListArray }
    });

    if (opts.peersOnly) return;

    // 2. Broadcast to all local clients
    const clientListArray = masterListArray.map(c => ({
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
}
