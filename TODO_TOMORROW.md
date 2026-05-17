# Plan for Tomorrow — SD TP2 Final Push

**Goal:** Pass every test in the TP2 tester (108a/108b, then 109–116 replication, then security).
**Deadline:** May 26th, 23h59.

---

## Status Recap (end of session 2026-05-17)

| Test range    | Status                                                      |
|---------------|-------------------------------------------------------------|
| 101a–107a     | ✅ All passing                                               |
| 108a          | ⚠️ Fix applied — needs rebuild + test run to confirm         |
| 108b          | ⚠️ Depends on 108a passing                                   |
| 109–116       | ❌ Not started (F2b replication)                             |

---

## Step 1 — Rebuild and test 108a / 108b

The `remotePostMessage` ordering fix was applied at end of session 2026-05-17.
**Do not re-apply it** — just rebuild the image and run the tests.

```powershell
cd "C:\Users\anaom\IdeaProjects\sd2526.trab2"
mvn clean compile package -q
docker build -t sd2526-trab2 .
```

Then run 108a:
```bash
docker run --rm --network=sdnet -v //var/run/docker.sock:/var/run/docker.sock \
  nunopreguica/sd2526-tester-tp2:latest -image sd2526-trab2 -test 108a
```

Expected: both sub-tests (delivery + restart) pass.

Then run 108b:
```bash
docker run --rm --network=sdnet -v //var/run/docker.sock:/var/run/docker.sock \
  nunopreguica/sd2526-tester-tp2:latest -image sd2526-trab2 -test 108b
```

### What the fix does (for reference)
`remotePostMessage` in `JavaMessagesExternal.java` now writes to Zoho **before** updating
the in-memory inbox. This means `getAllInboxMessages` returns empty until the Zoho write
completes — so the tester's "success" signal (seeing the message in the inbox) can only
arrive after durable storage. The container is killed only after tester success → Zoho
already has the message when Phase 2 starts.

---

## Step 2 — F2b Replication (tests 109–116, after 108a/b pass)

After E1 is working, implement the replicated Messages server.

### What already exists
- `messages.props` already has:
  ```
  MESSAGES_REP_SERVER_MAINCLASS=sd2526.trab.impl.rest.servers.RestMessagesRepServer
  MESSAGES_REP_PORT=5678
  MESSAGES_REP_EXTRA_ARGS_FIRST=primary
  MESSAGES_REP_EXTRA_ARGS_OTHER=secondary
  ```
- `RestMessagesRepServer` does NOT yet exist — needs to be created.

### Design summary (from CLAUDE.md)
- `RestMessagesRepServer` — entry point; `args[0]` = "primary" or "secondary"
- Primary maintains a sequence-numbered operation log
- On write: assign seq → apply locally → replicate to all known secondaries via `/admin/replicate`
- On read: serve from local state, respect `X-MESSAGES-VERSION` header
- Secondaries register with primary on startup; on restart, request replay from last known seq
- `X-MESSAGES-VERSION` response header = current seq number (via JAX-RS `ContainerResponseFilter`)

See `CLAUDE.md` → "Replication Design Notes" section for full detail.

Tests covered (F2b):
- 109a–d: basic replication (1 domain, 3 replicas)
- 110a–d: secondary failure (no restart)
- 111a: secondary restart
- 112a–b: primary failure (reads only — no masking needed for F2b)
- 113a–c: multi-domain basic replication
- 114a–c: multi-domain secondary failure
- 115a–b: multi-domain secondary restart
- 116a–b: multi-domain primary failure (reads only)

---

## Step 3 — Security (shared secret)

Add server-to-server shared secret authentication for `remotePostMessage`, `remoteDeleteMessage`, `remoteDeleteUserInbox`.

- Secret is passed as a startup arg to each server
- Receiving server rejects calls that don't include the secret

Low priority — do last.

---

## Quick Reference

### Build commands
```powershell
# Compile + package only
mvn clean compile package -q

# Build Docker image (must be done from project root)
docker build -t sd2526-trab2 .

# Run specific test
docker run --rm --network=sdnet -v //var/run/docker.sock:/var/run/docker.sock \
  nunopreguica/sd2526-tester-tp2:latest -image sd2526-trab2 -test 108a

# Run full suite
docker run --rm --network=sdnet -v //var/run/docker.sock:/var/run/docker.sock \
  nunopreguica/sd2526-tester-tp2:latest -image sd2526-trab2

# Stop all containers
docker ps -q | xargs docker stop
```

### Key file locations
| File | Purpose |
|------|---------|
| `src/.../utils/ZohoMailClient.java` | Zoho HTTP wrapper (TLS trust-all, 15s timeout) |
| `src/.../servers/JavaMessagesExternal.java` | Business logic (Zoho-backed) — **needs remotePostMessage fix above** |
| `src/.../servers/RestMessagesExternalServer.java` | Entry point (port 4568, parses clearState from args[0]) |
| `src/.../servers/RestMessagesResource.java` | Modified — has `setExternalImpl()` injection hook |
| `messages.props` | `MESSAGES_REST_PROXY_SERVER_MAINCLASS` → `RestMessagesExternalServer` |

### Zoho credentials (baked into ZohoMailClient)
```
CLIENT_ID:     1000.I1JQCAHZD3YQ230GLVNHVYI4NBMOTI
CLIENT_SECRET: 067d7bbd219cae50ea80e7749c093dc079d4563073
REFRESH_TOKEN: 1000.c1c4b0c562be5a06006d674d43d106bd.8c1ad57014b49665606a4f29d3054512
ACCOUNT_ID:    8670264000000002002
EMAIL:         sd2526messages@zohomail.eu
API_BASE:      https://mail.zoho.eu
TOKEN_URL:     https://accounts.zoho.eu/oauth/v2/token
```
Uses trust-all SSL context (needed because Docker base image JVM truststore lacks commercial CAs).
Emails stored in **Sent** folder; listed via `folder=Sent`.

### Key runtime facts
- Proxy port = 4568, regular Messages port = 4567, replication port = 5678
- `MESSAGES_REST_PROXY_EXTRA_ARGS=` (empty — tester injects clearState flag as args[0])
- Tester gives 9 seconds for server startup before making requests
- Proxy domain constructor waits max 7 seconds for Zoho init (join timeout)
- TLS: Docker generates keystores at build time via `generate-keystores.sh`
