import * as jwt from 'jsonwebtoken';

/**
 * Verifying credentials for the mesh's own control plane.
 *
 * The counterpart to central/src/mesh-auth.ts, which explains the scheme and
 * what it does and does not buy. In short: central holds this node's validation
 * key, so anything signed with it came from central, and a token names the one
 * node it is good for and the one thing it permits.
 *
 * This node signs nothing for other nodes. It only verifies what central minted
 * for it, and presents - unaltered - what central minted for somebody else.
 */

export type MeshScope = 'relay' | 'peer' | 'cluster-connect' | 'authority' | 'room-delete';

/**
 * Whether a missing or bad credential refuses the request.
 *
 * `enforce` is the default and the point of the exercise. `warn` exists for one
 * situation: a rolling upgrade, where a node running the new code is peered
 * with one that has not been restarted yet and therefore sends no token.
 * Enforcing during that window severs the peer link, and a severed peer link is
 * how a room ends up with players on a replica that never reach the authority.
 *
 * It is a deployment aid with a real cost, so it says so on every rejection it
 * lets through, and there is no third mode that quietly permits.
 */
const MODE = process.env.MESH_AUTH_MODE === 'warn' ? 'warn' : 'enforce';

export const meshAuthMode = MODE;

export interface MeshAuthOutcome {
  ok: boolean;
  /** Set when the credential failed; absent on success. */
  reason?: string;
}

let getKey: () => string | null = () => null;
let getSelfId: () => string | null = () => null;

/**
 * Wire up the accessors for this node's identity.
 *
 * Injected rather than imported because the values live in index.ts, which
 * imports this module - and a cycle between them resolves to `undefined` at the
 * moment of use rather than at startup, which is the kind of failure that shows
 * up as "authentication is broken in production only".
 */
export function initMeshAuth(accessors: {
  validationKey: () => string | null;
  nodeId: () => string | null;
}): void {
  getKey = accessors.validationKey;
  getSelfId = accessors.nodeId;
}

/**
 * Check a token against a scope, and a room where the scope is room-specific.
 *
 * The room check is not decoration. Without it a replica legitimately holding a
 * relay credential for its own room could relay into any other room on the same
 * authority, which is most of what the credential was meant to prevent.
 */
export function verifyMeshToken(
  token: string | undefined,
  scope: MeshScope,
  roomId?: string
): MeshAuthOutcome {
  const key = getKey();
  if (!key) {
    // Before registration completes this node has no key, so it cannot verify
    // anything. Refusing is correct: it has also not been assigned any rooms
    // yet, so there is nothing legitimate to refuse.
    return { ok: false, reason: 'node has no validation key yet' };
  }
  if (!token) return { ok: false, reason: 'no token' };

  let decoded: { scope?: string; aud?: string; roomId?: string };
  try {
    decoded = jwt.verify(token, key) as typeof decoded;
  } catch (err) {
    return { ok: false, reason: `invalid token: ${(err as Error).message}` };
  }

  if (decoded.scope !== scope) {
    return { ok: false, reason: `token is for scope ${decoded.scope}, needed ${scope}` };
  }

  const selfId = getSelfId();
  if (selfId && decoded.aud !== selfId) {
    return { ok: false, reason: `token is addressed to ${decoded.aud}, not ${selfId}` };
  }

  if (roomId !== undefined && decoded.roomId !== roomId) {
    return { ok: false, reason: `token is for room ${decoded.roomId}, not ${roomId}` };
  }

  return { ok: true };
}

/**
 * Prove to central that we are this node.
 *
 * The only thing this node signs. Central verifies it with the copy of the
 * validation key it issued at registration, which is why possession of that key
 * is what "is this node" means. Scoped `self` so it can never be mistaken for
 * one of the capability tokens central mints - see the matching check in
 * central/src/mesh-auth.ts.
 */
export function selfAuthHeaders(): Record<string, string> {
  const key = getKey();
  const id = getSelfId();
  if (!key || !id) return {};
  const token = jwt.sign({ scope: 'self', sub: id }, key, { expiresIn: 60 });
  return { 'x-node-id': id, Authorization: `Bearer ${token}` };
}

/** Pull a bearer token out of a request's headers. */
export function bearerToken(headers: Record<string, unknown>): string | undefined {
  const header = headers['authorization'];
  if (typeof header !== 'string' || !header.startsWith('Bearer ')) return undefined;
  return header.slice('Bearer '.length).trim() || undefined;
}

/**
 * Guard a Fastify route. Returns true when the caller may proceed.
 *
 * Replies with 401 itself so a route body reads as `if (!requireMeshAuth(...))
 * return;` - the shape the existing token check on /api/rooms/:roomId/clients
 * already uses.
 */
export function requireMeshAuth(
  request: { headers: Record<string, unknown> },
  reply: { code: (n: number) => { send: (body: unknown) => unknown } },
  scope: MeshScope,
  what: string,
  roomId?: string
): boolean {
  const outcome = verifyMeshToken(bearerToken(request.headers), scope, roomId);
  if (outcome.ok) return true;

  if (MODE === 'warn') {
    console.warn(
      `[MESH-AUTH] PERMITTING unauthenticated ${what} (${outcome.reason}) because ` +
        `MESH_AUTH_MODE=warn. This request would be refused in enforce mode.`
    );
    return true;
  }

  console.warn(`[MESH-AUTH] Refused ${what}: ${outcome.reason}`);
  reply.code(401).send({ error: 'Mesh credential required', details: outcome.reason });
  return false;
}
