# arrr-node

The open-source mesh node for the [ARRR network](https://arrr.fun).
Run one on your own machine or server and it hosts rooms for multiplayer
applications built on the network: it sequences inputs, replicates rooms to
peer nodes, relays voice, and serves late joiners a snapshot.

A node does nothing on its own. It registers with a cloud service
([arrr-cloud](https://github.com/m0dE/arrr-cloud), hosted at
`https://nodes.arrr.fun`), which is what sends clients to it and connects it
to other nodes. To provide a node to the public network you need an account
there and a **node token** from its portal.

## Requirements

- Node.js 18 or newer.
- A public address. The cloud service calls the node back to verify it,
  browsers open WebSockets to it, and other nodes relay through it. Behind a
  home router that means forwarding the port, or running a tunnel
  (Cloudflare Tunnel, ngrok, Tailscale Funnel) that gives you a public
  hostname with TLS.

## Quick start

```bash
git clone https://github.com/m0dE/arrr-node.git
cd arrr-node
npm install
cp .env.example .env     # then fill in the values below
npm start
```

`npm start` builds and runs `dist/index.js`. `npm run dev` runs from source
with a file watcher.

| Variable | Default | Notes |
|----------|---------|-------|
| `ARRR_NODE_TOKEN` | *(unset)* | Token from the portal's **Nodes** page. Registers the node under your account. A service running `NODE_REGISTRATION=token` refuses nodes without one. |
| `CENTRAL_SERVICE_URL` | `http://localhost:9001` | The cloud service to register with. `https://nodes.arrr.fun` for the public network. |
| `NODE_NAME` | `Node` | Label shown in the portal and dashboard. |
| `NODE_PORT` | `8001` | Port to listen on. |
| `NODE_PUBLIC_URL` | `ws://localhost:$NODE_PORT/ws` | Where browsers connect. Must be reachable from the internet. |
| `NODE_API_PUBLIC_URL` | `http://localhost:$NODE_PORT` | Where the cloud service and other nodes reach this node's HTTP API. Usually the same host as above with `http(s)://` and no `/ws`. |
| `NODE_ID` | ephemeral | Leave unset. A fresh id per process is what lets the service replace a restarted node's stale registration. |
| `MESH_AUTH_MODE` | `enforce` | `warn` permits, and logs, control-plane calls that would otherwise be refused. Only for a rolling upgrade. |
| `GHOST_GRACE_MS` | `120000` | How long a member is kept after their socket drops for no stated reason. |
| `UNLOAD_GRACE_MS` | `15000` | The same, for a client whose page said it was unloading. |
| `GHOST_SWEEP_MS` | `5000` | How often expired members are swept. |
| `VOICE_RANGE_M` / `VOICE_MAX_SPEAKERS` | `50` / `8` | Voice relay radius and fan-out. |

## What happens when it starts

1. The node listens on `NODE_PORT` and posts its id, name and URLs to
   `CENTRAL_SERVICE_URL/api/nodes`, with `ARRR_NODE_TOKEN` as a bearer token
   if set.
2. The service calls back `NODE_API_PUBLIC_URL/api/auth/verify` to check that
   the process at that address is the one that asked. If that URL is not
   reachable from the service, registration fails here.
3. The service answers with a validation key. The node uses it to verify the
   join tokens clients present, and mesh tokens from other nodes.
4. Every ten seconds the node heartbeats. If the service has forgotten it
   (a restart, say) it registers again by itself.

A refused registration is logged with the service's reason and retried every
five seconds, so a token created in the portal after the node started is
picked up without a restart.

`GET /health` on the node reports what it is holding. `GET /api/stats` is
what the dashboard reads.

## Development against a local service

Run [arrr-cloud](https://github.com/m0dE/arrr-cloud) locally (`npm run dev`,
port 9001) and start the node with no token and no public URLs. The service's
default `NODE_REGISTRATION=open` accepts it.

This repository is published from a private monorepo where the node, the
cloud service and the SDK are developed together with the test suites that
run all three. Issues and pull requests are welcome here: accepted changes
are applied in the monorepo and appear in the next publish, with credit.

## License

MIT.
