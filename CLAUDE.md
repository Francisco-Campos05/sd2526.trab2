# SD 2526 — Assignment 2 Project Memory

## Deadline
May 26th, 23h59 (1 hour tolerance)

---

## Features to Implement

### 1. Security (4 pts) — IN PROGRESS
TLS is already wired up (`TLSUtils`, `AbstractGrpcServer`, `AbstractRestServer`).
What is still missing:
- **Server-to-server shared secret authentication**: when one Messages server calls
  another (e.g. `remotePostMessage`, `remoteDeleteMessage`), it must pass a shared
  secret as a parameter so the receiving server can reject calls that come from
  clients pretending to be servers.
- The shared secret is passed as a startup argument to each server.

### 2. External Service — E1: Zoho Mail (6 pts) — IMPLEMENTED

All three new files and both modifications are committed and working.
108a Phase 1 (cross-domain delivery) passes. 108a Phase 2 (restart persistence) fix applied
2026-05-17 — needs a rebuild + test run to confirm.

#### Files in place

| File | Status |
|------|--------|
| `src/sd2526/trab/impl/utils/ZohoMailClient.java` | ✅ Done |
| `src/sd2526/trab/impl/java/servers/JavaMessagesExternal.java` | ✅ Done |
| `src/sd2526/trab/impl/rest/servers/RestMessagesExternalServer.java` | ✅ Done |
| `src/sd2526/trab/impl/rest/servers/RestMessagesResource.java` | ✅ Modified (setExternalImpl) |
| `messages.props` | ✅ Modified (proxy mainclass + port 4568) |

#### Design notes

- Messages stored as self-addressed emails in Zoho Sent folder.
  Subject: `SD2526:<messageId>`. Body: full JSON of `Message` object (Jackson).
- `ZohoMailClient` uses `java.net.http.HttpClient` with a **trust-all SSL context** — required
  because the Docker base image JVM truststore lacks commercial CAs (PKIX error otherwise).
- Token is refreshed lazily, cached with 60-second expiry buffer.
- Zoho init runs in a daemon background thread, joined for max 7 seconds so startup is fast.
- `remotePostMessage`: Zoho write happens **BEFORE** inbox update. This ensures `getAllInboxMessages`
  returns the message only after it is durably stored in Zoho, so the tester's "success" signal
  cannot arrive before the Zoho write completes.

#### Zoho credentials (baked into ZohoMailClient)
```
CLIENT_ID:     1000.I1JQCAHZD3YQ230GLVNHVYI4NBMOTI
CLIENT_SECRET: 067d7bbd219cae50ea80e7749c093dc079d4563073
REFRESH_TOKEN: 1000.c1c4b0c562be5a06006d674d43d106bd.8c1ad57014b49665606a4f29d3054512
ACCOUNT_ID:    8670264000000002002
EMAIL:         sd2526messages@zohomail.eu
API_BASE:      https://mail.zoho.eu
TOKEN_URL:     https://accounts.zoho.eu/oauth/v2/token
```

#### Zoho API reference

| Operation | Method | URL |
|-----------|--------|-----|
| Refresh token | POST | `https://accounts.zoho.eu/oauth/v2/token` |
| List emails | GET | `{API_BASE}/api/accounts/{ACCOUNT_ID}/messages/view?folder=Sent&limit=200` |
| Get body | GET | `{API_BASE}/api/accounts/{ACCOUNT_ID}/folders/{folderId}/messages/{zohoId}/content` |
| Send email | POST | `{API_BASE}/api/accounts/{ACCOUNT_ID}/messages` |
| Delete email | DELETE | `{API_BASE}/api/accounts/{ACCOUNT_ID}/folders/{folderId}/messages/{zohoId}` |

All requests: `Authorization: Zoho-oauthtoken {token}`, `Accept: application/json`

### 3. Fault Tolerance — F2b: Primary/Secondary, no primary fault masking (7 pts)
*(Could upgrade to F2a for 10 pts — see notes below)*

Replicate the Messages REST server using the **primary/secondary protocol**.

Rules for F2b:
- Must tolerate failure of **any secondary** server (including restart).
- If the **primary fails**: reads still work from secondaries, writes stop (not required to mask).
- The solution must support replication in **different domains** (multi-domain).
- Servers must expose a **REST interface**.
- Monotonic reads: responses include `X-MESSAGES-*` headers with a version number.
  The tester resends these headers on subsequent requests; the server must not
  return a version older than the one the client has seen.

Key implementation points:
- Primary maintains a **sequence-numbered operation log**.
- On write: primary assigns seq number → applies locally → replicates to all secondaries.
- On read: any replica can serve, but must respect the version in the `X-MESSAGES` header.
- Secondary restart recovery: secondary asks primary for all operations it missed
  (replay from log since last known seq number).
- Configured via `MESSAGES_REP_SERVER_MAINCLASS` / `MESSAGES_REP_PORT` in `messages.props`.
- First server arg: `primary` or `secondary` (already in `messages.props`).
  ```
  MESSAGES_REP_EXTRA_ARGS_FIRST=primary
  MESSAGES_REP_EXTRA_ARGS_OTHER=secondary
  ```

**If upgrading to F2a (10 pts)**, additionally need:
- ZooKeeper-based leader election when primary crashes.
- Promote a secondary to primary automatically.
- Handle in-flight writes during failover.

#### Test coverage by choice:
| Tests | F2b | F2a |
|-------|-----|-----|
| 109a–d (basic replication, 1 domain 3 replicas) | ✅ | ✅ |
| 110a–d (secondary failure, no restart) | ✅ | ✅ |
| 111a (secondary failure with restart) | ✅ | ✅ |
| 112a–b (primary failure) | ❌ reads only | ✅ |
| 113a–c (multi-domain basic replication) | ✅ | ✅ |
| 114a–c (multi-domain secondary failure) | ✅ | ✅ |
| 115a–b (multi-domain secondary restart) | ✅ | ✅ |
| 116a–b (multi-domain primary failure) | ❌ reads only | ✅ |

---

## Current Test Status (as of 2026-05-17)

### Passing (verified)
- 101a, 101b, 101c — discovery + TLS
- 102a, 102b, 102c — REST Users (single domain)
- 103a, 103b, 103c, 103d — REST Messages (single domain)
- 104a, 104b, 104c — GRPC Messages (single domain)
- 105a, 105b, 105c, 105d, 105e — multi-domain messages (REST + GRPC)
- 106a, 106b — mixed REST+GRPC domains
- 107a — Gateway
- 108a Phase 1 — cross-domain delivery to Zoho proxy (verified passing)

### Needs rebuild + test (fix applied, not yet verified)
- **108a Phase 2 / 108b** — proxy restart persistence. Fix applied 2026-05-17:
  `remotePostMessage` now writes Zoho BEFORE updating inbox so the tester's "success"
  only fires after durable storage. Next step: `docker build -t sd2526-trab2 .` then run 108a.

### Not yet implemented
- **109–116** — F2b primary/secondary replication (`RestMessagesRepServer`)

---

## Known Bugs — FIXED

### FIXED: gRPC server startup race (tests 104b, 104c)
**Root cause**: Both gRPC servers (Users and Messages) had announce-before-start
ordering and blocked their constructors on Hibernate init. On slower runs, the
tester's first PostMessage arrived before the messages server finished starting.
**Fix applied**:
- Removed `Hibernate.getInstance()` from `JavaMessages` constructor (no longer blocks)
- Added background warmup thread in `GrpcMessagesServer.main()` and `GrpcUsersServer.main()`
- Added `start()` override in both servers to announce AFTER `server.start()`
- `AbstractGrpcServer.start()` keeps announce-before-start for other servers (messages
  server construction still takes a few seconds even without Hibernate, so the gap is tiny)

**Files changed**:
- `GrpcUsersServer.java` — background warmup thread + start() override
- `GrpcMessagesServer.java` — background warmup thread + start() override
- `JavaMessages.java` — removed Hibernate.getInstance() from constructor
- `RestMessagesServer.java` — explicitly calls Hibernate.getInstance() in main()

---

## Architecture Notes

### Server types and ports (from messages.props)
| Server | Class | Port |
|--------|-------|------|
| Users REST | `RestUsersServer` | 3456 |
| Users GRPC | `GrpcUsersServer` | 13456 |
| Messages REST | `RestMessagesServer` | 4567 |
| Messages GRPC | `GrpcMessagesServer` | 14567 |
| Gateway REST | `RestGatewayServer` | 6666 |
| Messages External (proxy/E1) | `RestMessagesExternalServer` *(to create)* | 4568 |
| Messages Replicated (F2b) | `RestMessagesRepServer` *(to create)* | 5678 |

### Key source files
- `src/sd2526/trab/impl/java/servers/JavaMessages.java` — core message logic
- `src/sd2526/trab/impl/java/servers/JavaUsers.java` — core user logic (Bug 1 here)
- `src/sd2526/trab/impl/java/servers/JavaBaseService.java` — shared domain utilities
- `src/sd2526/trab/impl/java/clients/Clients.java` — client factories
- `src/sd2526/trab/impl/java/clients/ClientFactory.java` — discovery-backed client
- `src/sd2526/trab/impl/discovery/Discovery.java` — UDP multicast discovery
- `src/sd2526/trab/impl/grpc/servers/AbstractGrpcServer.java` — gRPC server base
- `src/sd2526/trab/impl/rest/servers/AbstractRestServer.java` — REST server base

### Keystores (already generated for all servers)
Users: `users.ourorg0`, `users.ourorg1`, `users.ourorg2`
Messages: `messages0-2.ourorg0`, `messages0-2.ourorg1`, `messages.ourorg2`
Client truststore: `client-truststore.ks` (password: `changeme`)
All keystore passwords: `changeme`

### Discovery
- Multicast group: `226.226.226.226:2266`
- Service names: `Users@<domain>`, `Messages@<domain>`
- URIs end in `/rest` (REST) or `/grpc` (gRPC) — `ClientFactory` uses this suffix
  to choose the right client implementation.

---

## Replication Design Notes (F2b)

The `ReplicationManager` class should:
1. Maintain an `AtomicLong sequenceNumber` for ordering.
2. On write operations: assign seq number, apply locally, then replicate to all
   known secondaries via an admin endpoint (`/admin/replicate`).
3. On read: check the `X-MESSAGES-VERSION` header from the request; if the local
   version is behind, either wait or redirect to primary.
4. Secondaries register themselves with the primary on startup.
5. Primary maintains a log (list of `ReplicatedOperation` objects with seq + method + params).
6. On secondary reconnect/restart: secondary sends its last known seq to the primary,
   primary replays missing operations.

The `X-MESSAGES-VERSION` header value = the current sequence number of the server.
Use a JAX-RS `ContainerResponseFilter` to inject it on every response (Alternative 3
from the replication notes: `ThreadLocal<Long>` + dual filter for request+response).

---

## Build & Test Commands

```bash
# Compile + package (from project root)
mvn clean compile package -q

# Build Docker image (MUST use direct docker build — mvn docker:build is broken due to image name)
docker build -t sd2526-trab2 .

# Run full TP2 test suite
docker run --rm --network=sdnet -v //var/run/docker.sock:/var/run/docker.sock \
  nunopreguica/sd2526-tester-tp2:latest -image sd2526-trab2

# Run specific test
docker run --rm --network=sdnet -v //var/run/docker.sock:/var/run/docker.sock \
  nunopreguica/sd2526-tester-tp2:latest -image sd2526-trab2 -test 108a

# Stop all running containers
docker ps -q | xargs docker stop
```
