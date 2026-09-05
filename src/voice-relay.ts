/**
 * Voice relay: who hears whom.
 *
 * Voice is the one kind of traffic that cannot go through the ordered input
 * stream. A microphone is not deterministic, nobody replays it, and it must
 * not be held up behind a tick. So it rides the same socket as everything
 * else but on its own message type, outside sequencing, and the node does
 * for it what a media server does: takes one stream in from each talker and
 * fans it out - not to everyone, but to the people close enough to hear.
 *
 * That last part is what makes a room of hundreds cost the same per person
 * as a room of five. Each client uploads one stream, and downloads at most
 * VOICE_MAX_SPEAKERS, chosen by distance among whoever is actually talking.
 * The node never runs the application's simulation: clients say where they
 * are in every packet, and quiet clients say it with an empty one every so
 * often, so a listener has a position even when they have never spoken.
 *
 * Wire format, client -> node, type 0x40:
 *
 *   [0x40][seq:2 LE][x:f32][y:f32][z:f32][len:2 LE][payload:len]
 *
 * A zero-length payload is a position beacon and is never forwarded. The
 * payload itself is opaque - the reference client sends 20ms Opus frames -
 * and the node never looks inside it.
 *
 * Node -> client, type 0x41:
 *
 *   [0x41][idLen:1][from:idLen utf8][seq:2 LE][x:f32][y:f32][z:f32][len:2 LE][payload]
 *
 * `from` is the member id the talker joined as (user.id), which is the name
 * every client already knows the talker by, rather than the connection id.
 *
 * Across nodes a packet is relayed once to each peer as RELAY_VOICE, and the
 * peer fans it out to its own listeners by the same rule. Positions travel
 * with the packet, so a peer needs nothing but its own clients' beacons.
 */
import type WebSocket from 'ws';
import { roomManager } from './room-manager';
import { MessageType } from './types';
import type { PeerManager } from './peer-manager';

/** Past this many metres a voice is not delivered at all. */
export const VOICE_RANGE = Number(process.env.VOICE_RANGE_M || 30);
/** The most voices any one listener is sent at once. */
export const VOICE_MAX_SPEAKERS = Number(process.env.VOICE_MAX_SPEAKERS || 8);
/** A talker with no packet for this long is silent, and drops out of rankings. */
const SPEAKING_TTL_MS = 600;
/** How often the who-hears-whom sets are rebuilt, per room. */
const RANK_EVERY_MS = 200;
/** A position older than this is unknown; the client is probably gone. */
const POSITION_TTL_MS = 30_000;
/** 20ms of Opus at 32kbps is ~80 bytes. Anything near this ceiling is not voice. */
const MAX_VOICE_BYTES = 1024;
/** Packets per second per client, sustained; 50 is one 20ms frame per 20ms. */
const RATE_PER_SEC = 60;
const BURST = 60;

export const VOICE_TYPE = 0x40;
export const VOICE_FROM_TYPE = 0x41;
/**
 * Sent once to every client that joins or creates a room. A client that has
 * not seen it must not send voice: an older node would treat the packet as
 * junk, and say so in its log fifty times a second per talker.
 */
export const VOICE_READY_PACKET = Buffer.from([0x42]);
const HEADER = 1 + 2 + 12 + 2;

interface Presence {
  /** connection id for local clients; `peer:<clientId>` for remote talkers */
  key: string;
  odId: string;
  x: number; y: number; z: number;
  seenAt: number;      // last beacon or packet
  spokeAt: number;     // last non-empty packet
  socket?: WebSocket;  // local clients only
  tokens: number; tokensAt: number;
}

interface RoomVoice {
  presence: Map<string, Presence>;
  /** listener key -> the talker keys it currently receives */
  hears: Map<string, Set<string>>;
  rankedAt: number;
}

const rooms = new Map<string, RoomVoice>();

const stats = {
  packetsIn: 0, packetsOut: 0, beacons: 0, dropped: 0, relayedOut: 0, relayedIn: 0
};

function roomVoice(roomId: string): RoomVoice {
  let rv = rooms.get(roomId);
  if (!rv) {
    rv = { presence: new Map(), hears: new Map(), rankedAt: 0 };
    rooms.set(roomId, rv);
  }
  return rv;
}

function presenceFor(rv: RoomVoice, key: string, odId: string, socket?: WebSocket): Presence {
  let p = rv.presence.get(key);
  if (!p) {
    p = { key, odId, x: NaN, y: NaN, z: NaN, seenAt: 0, spokeAt: 0, socket, tokens: BURST, tokensAt: Date.now() };
    rv.presence.set(key, p);
  }
  if (socket) p.socket = socket;
  if (odId) p.odId = odId;
  return p;
}

function overBudget(p: Presence, now: number): boolean {
  p.tokens = Math.min(BURST, p.tokens + ((now - p.tokensAt) / 1000) * RATE_PER_SEC);
  p.tokensAt = now;
  if (p.tokens >= 1) { p.tokens -= 1; return false; }
  return true;
}

function dist2(a: Presence, b: Presence): number {
  const dx = a.x - b.x, dy = a.y - b.y, dz = a.z - b.z;
  return dx * dx + dy * dy + dz * dz;
}

/**
 * Rebuild who hears whom.
 *
 * For every local listener: the talkers that spoke recently, nearest first,
 * within range, at most VOICE_MAX_SPEAKERS of them. A listener whose position
 * is unknown gets the first few talkers regardless of distance - hearing
 * something is better than the silence that would otherwise last until its
 * first beacon arrives.
 */
function rank(rv: RoomVoice, now: number): void {
  rv.rankedAt = now;
  const talkers: Presence[] = [];
  for (const p of rv.presence.values()) {
    if (now - p.seenAt > POSITION_TTL_MS && !p.socket) { rv.presence.delete(p.key); continue; }
    if (now - p.spokeAt <= SPEAKING_TTL_MS) talkers.push(p);
  }
  const range2 = VOICE_RANGE * VOICE_RANGE;
  for (const listener of rv.presence.values()) {
    if (!listener.socket) continue;
    const placed = !isNaN(listener.x);
    const ranked: { t: Presence; d: number }[] = [];
    for (const t of talkers) {
      if (t === listener || t.odId === listener.odId) continue;
      const d = placed && !isNaN(t.x) ? dist2(listener, t) : 0;
      if (placed && d > range2) continue;
      ranked.push({ t, d });
    }
    ranked.sort((a, b) => a.d - b.d);
    const set = new Set<string>();
    for (let i = 0; i < ranked.length && i < VOICE_MAX_SPEAKERS; i++) set.add(ranked[i].t.key);
    rv.hears.set(listener.key, set);
  }
  for (const key of rv.hears.keys()) {
    if (!rv.presence.has(key)) rv.hears.delete(key);
  }
}

/**
 * Deliver one talker's packet to the local listeners that should hear it.
 *
 * Between rankings a listener with a free slot admits a new talker within
 * range immediately, so the first word somebody says is not lost to the
 * 200ms until the next rebuild.
 */
function fanOut(rv: RoomVoice, talker: Presence, packet: Buffer, now: number): void {
  if (now - rv.rankedAt >= RANK_EVERY_MS) rank(rv, now);
  const range2 = VOICE_RANGE * VOICE_RANGE;
  for (const listener of rv.presence.values()) {
    if (!listener.socket || listener === talker || listener.odId === talker.odId) continue;
    let set = rv.hears.get(listener.key);
    if (!set) { set = new Set(); rv.hears.set(listener.key, set); }
    if (!set.has(talker.key)) {
      if (set.size >= VOICE_MAX_SPEAKERS) continue;
      if (!isNaN(listener.x) && !isNaN(talker.x) && dist2(listener, talker) > range2) continue;
      set.add(talker.key);
    }
    if (listener.socket.readyState !== 1) continue;
    try { listener.socket.send(packet); stats.packetsOut++; } catch { /* closing */ }
  }
}

function encodeFrom(from: string, seq: number, x: number, y: number, z: number, payload: Buffer): Buffer {
  const id = Buffer.from(from, 'utf8').subarray(0, 255);
  const out = Buffer.allocUnsafe(1 + 1 + id.length + 2 + 12 + 2 + payload.length);
  let at = 0;
  out[at++] = VOICE_FROM_TYPE;
  out[at++] = id.length;
  id.copy(out, at); at += id.length;
  out.writeUInt16LE(seq, at); at += 2;
  out.writeFloatLE(x, at); at += 4;
  out.writeFloatLE(y, at); at += 4;
  out.writeFloatLE(z, at); at += 4;
  out.writeUInt16LE(payload.length, at); at += 2;
  payload.copy(out, at);
  return out;
}

export const voiceRelay = {
  /**
   * A client's own packet. `odId` is the member it joined as; `muted` is the
   * room's moderation verdict, which applies to speech more literally than
   * to anything else it gates.
   */
  onPacket(roomId: string, clientId: string, odId: string, socket: WebSocket, data: Buffer,
    muted: boolean, peerManager: PeerManager | null): void {
    if (data.length < HEADER) return;
    const len = data.readUInt16LE(HEADER - 2);
    if (data.length !== HEADER + len || len > MAX_VOICE_BYTES) { stats.dropped++; return; }
    const now = Date.now();
    const rv = roomVoice(roomId);
    const me = presenceFor(rv, clientId, odId, socket);
    me.x = data.readFloatLE(3); me.y = data.readFloatLE(7); me.z = data.readFloatLE(11);
    me.seenAt = now;
    if (len === 0) { stats.beacons++; return; }
    if (muted) { stats.dropped++; return; }
    if (overBudget(me, now)) { stats.dropped++; return; }
    stats.packetsIn++;
    me.spokeAt = now;
    const seq = data.readUInt16LE(1);
    const payload = data.subarray(HEADER);
    fanOut(rv, me, encodeFrom(odId, seq, me.x, me.y, me.z, payload), now);
    if (peerManager && peerManager.hasConnectedPeers()) {
      stats.relayedOut++;
      peerManager.broadcastToPeers({
        type: MessageType.RELAY_VOICE,
        payload: { roomId, clientId, from: odId, seq, x: me.x, y: me.y, z: me.z, data: payload.toString('base64') }
      });
    }
  },

  /** A packet from a talker on another node, already fanned out there. */
  onRelayed(payload: any): void {
    if (!payload || typeof payload.roomId !== 'string' || typeof payload.from !== 'string') return;
    const room = roomManager.getRoom(payload.roomId);
    if (!room || room.clients.size === 0) return;
    const data = Buffer.from(String(payload.data || ''), 'base64');
    if (data.length === 0 || data.length > MAX_VOICE_BYTES) return;
    stats.relayedIn++;
    const now = Date.now();
    const rv = roomVoice(payload.roomId);
    const t = presenceFor(rv, 'peer:' + payload.clientId, payload.from);
    t.x = +payload.x; t.y = +payload.y; t.z = +payload.z;
    t.seenAt = now; t.spokeAt = now;
    fanOut(rv, t, encodeFrom(payload.from, payload.seq | 0, t.x, t.y, t.z, data), now);
  },

  /** The connection is gone; nothing more is heard from or by it. */
  forget(roomId: string, clientId: string): void {
    const rv = rooms.get(roomId);
    if (!rv) return;
    rv.presence.delete(clientId);
    rv.hears.delete(clientId);
    if (rv.presence.size === 0) rooms.delete(roomId);
  },

  /** For /health: how much voice is moving, and where. */
  stats() {
    let listeners = 0, talkers = 0;
    const now = Date.now();
    for (const rv of rooms.values()) {
      for (const p of rv.presence.values()) {
        if (p.socket) listeners++;
        if (now - p.spokeAt <= SPEAKING_TTL_MS) talkers++;
      }
    }
    return { ...stats, rooms: rooms.size, listeners, talkers, range: VOICE_RANGE, maxSpeakers: VOICE_MAX_SPEAKERS };
  },

  /** Test seam: current audible set for a listener. */
  hearing(roomId: string, clientId: string): string[] {
    const rv = rooms.get(roomId);
    const set = rv && rv.hears.get(clientId);
    return set ? Array.from(set) : [];
  }
};
