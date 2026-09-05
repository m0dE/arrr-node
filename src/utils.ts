import { randomBytes } from 'crypto';

/**
 * Convert a node's WebSocket connection URL into its HTTP API base URL.
 *
 * `wss://node1.example.com/ws` -> `https://node1.example.com`
 *
 * The protocol patterns are anchored so a URL that merely *contains* "ws://"
 * later in the path (e.g. a query string) is not corrupted, and the "/ws"
 * suffix is only stripped from the end.
 */
export function wsToHttp(url: string): string {
    return url
        .replace(/^wss:\/\//, 'https://')
        .replace(/^ws:\/\//, 'http://')
        .replace(/\/ws$/, '');
}

/**
 * The WebSocket address another node should dial to peer with this one.
 *
 * Derived from the node's own HTTP API rather than from the address clients
 * dial, because those are not always the same place. A deployment that tunnels
 * the node through the central service so a browser only needs one open port
 * advertises the tunnel as its connection URL - and the peer link is not a
 * browser. Dialling the tunnel lands on the central service as an ordinary
 * client, gets rejected for having no token, and the two nodes simply never
 * peer: measured as peers=0 on both sides, with every client on the replica
 * connected and frozen because the input stream never arrived.
 *
 * `http://localhost:8002` -> `ws://localhost:8002/ws/peer`
 */
export function peerUrlFromApi(apiUrl: string): string {
    const base = apiUrl.replace(/\/$/, '')
        .replace(/^https:\/\//, 'wss://')
        .replace(/^http:\/\//, 'ws://');
    return `${base}/ws/peer`;
}

/**
 * The peer socket address for a node, from whatever address you happen to hold.
 *
 * Accepts either the address clients dial (`ws://host/ws`) or an address that
 * is already a peer socket, and returns a peer socket either way. Idempotent on
 * purpose: callers now legitimately arrive with both forms, since the peer
 * address is derived from a node's own API when it publishes one and from the
 * client URL when it does not. Appending "/peer" to the first "/ws" found
 * regardless turned an already-complete `ws://host/ws/peer` into
 * `ws://host/ws/peer/peer`, which 404s - and the address in the log looked
 * perfectly correct and opened by hand, which cost an hour.
 */
export function peerUrlFor(url: string): string {
    if (/\/ws\/peer\/?$/.test(url)) return url.replace(/\/$/, '');
    if (/\/ws\/?$/.test(url)) return url.replace(/\/$/, '').replace(/\/ws$/, '/ws/peer');
    // Anything else - a tunnel path like /nodews, or a bare origin - gets the
    // peer route appended rather than pattern-substituted, because substitution
    // silently does nothing when the pattern is absent and leaves the caller
    // dialling a URL that was never a peer socket.
    return url.replace(/\/$/, '') + '/ws/peer';
}

/**
 * Why a room id is unusable, or null when it is fine.
 *
 * Ids arrive from whoever is connecting and were never checked. A room id is
 * not a one-off request: it is stored, logged on every join, sent to peers, and
 * written into the wire format with a UInt16 length field, so an id of tens of
 * kilobytes is a cost the room carries in every message about it, and one over
 * 65535 bytes cannot be encoded at all.
 *
 * That path is luckier than the input path that killed the node - it runs
 * inside the socket's message handler, which catches, so the failure stays with
 * the client that asked for it. Luckier is not bounded.
 *
 * Lives here rather than beside its callers so it can be tested without
 * dragging in a module that starts timers on import.
 */
export const MAX_ROOM_ID_LENGTH = Number(process.env.MAX_ROOM_ID_LENGTH || 256);

export function badRoomId(roomId: unknown): string | null {
    if (typeof roomId !== 'string' || roomId.length === 0) return 'a room id is required';
    if (roomId.length > MAX_ROOM_ID_LENGTH) {
        return `room id is ${roomId.length} characters, over the ${MAX_ROOM_ID_LENGTH} allowed`;
    }
    return null;
}

/**
 * Generate an identifier for a NetworkInput.
 *
 * Replaces the `Date.now()` + `Math.random().toString(36)` idiom that was
 * copy-pasted across the handlers: at high input rates many inputs share a
 * millisecond, and the random suffix was both short and variable-length
 * (`toString(36)` does not emit a fixed number of digits), so collisions were
 * plausible. Random bytes give a fixed-width, collision-resistant suffix.
 */
export function generateInputId(): string {
    return `input_${Date.now()}_${randomBytes(6).toString('hex')}`;
}
