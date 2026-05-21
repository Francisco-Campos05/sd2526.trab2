# Theoretical Overview — SD TP2 Extensions

This document explains the two major features added on top of the TP1 base: **TLS Security** and **F1 Kafka Replication**.

---

## 1. TLS Security

### Purpose
Transport Layer Security (TLS) protects all network communication between nodes — users servers, messages servers, gateways, and proxies — by providing:
- **Confidentiality**: data is encrypted so it cannot be read in transit
- **Integrity**: data cannot be tampered with without detection
- **Authentication**: each server proves its identity via a certificate, preventing impersonation attacks

Without TLS, any node on the same network could intercept or forge messages between servers, which is unacceptable in a production distributed system.

### How It Works

#### Certificates and Keystores
Each server has a **PKCS12 keystore** (`.ks` file) that contains:
- Its **private key** — kept secret, used to prove identity during the TLS handshake
- Its **self-signed certificate** — shared publicly, contains the server's public key

A single shared **truststore** (`client-truststore.ks`) contains the certificates of **all** servers. Every client (and server acting as a client) loads this truststore to know which certificates to trust.

The keystores are generated once inside the Docker image by `generate-keystores.sh` using Java's `keytool`, one per hostname (e.g. `users.ourorg0.ks`, `messages0.ourorg1.ks`).

#### TLS Handshake (simplified)
1. Client connects to server
2. Server sends its certificate
3. Client checks the certificate against its truststore — if the certificate is listed there, the server is trusted
4. Both sides negotiate a symmetric session key using asymmetric cryptography
5. All subsequent communication is encrypted with that session key

#### REST / HTTPS
REST servers use **Jetty** (embedded HTTP server). TLS is configured by passing a `KeyManager` (loaded from the server's own `.ks` file) to the `SslContextFactory`. The server then listens for HTTPS connections on the standard REST port.

Clients use **Jersey** with an `SSLContext` built from the shared truststore, which is loaded into a `TrustManager` and passed to the `HttpsURLConnection`.

#### gRPC / TLS
gRPC uses **Netty** as its transport layer. TLS is configured differently from REST:
- **Server side**: if a keystore file exists for the container hostname, `NettyServerBuilder` receives an `SslContext` built with `GrpcSslContexts.configure(SslContextBuilder.forServer(kmf))`. The server also announces itself with the `grpcs://` URI scheme so clients can detect TLS mode.
- **Client side**: `NettyChannelBuilder` receives an `SslContext` built with `GrpcSslContexts.forClient().trustManager(tmf)` — **only** when the server URI uses the `grpcs://` scheme. If the server announced `grpc://`, the client uses plaintext, avoiding a TLS handshake mismatch with a plaintext server.

#### URI Scheme Convention
- **`grpcs://host:port/grpc`** — TLS active (server keystore found); client uses TLS
- **`grpc://host:port/grpc`** — plaintext (no keystore); client uses plaintext

This scheme-based convention ensures client and server always agree on TLS mode regardless of which keystores happen to be bundled in the Docker image.

#### Key Classes Modified
| Class | Role |
|-------|------|
| `TLSUtils` | Loads keystores/truststores; provides `serverContext`, `getClientContext`, `keyManagerFactory`, `getTrustManagerFactory`, `keystoreExists` |
| `AbstractGrpcServer` | Uses `NettyServerBuilder` + `GrpcSslContexts` when server keystore exists; announces `grpcs://` when TLS |
| `GrpcClient` | Uses `NettyChannelBuilder` + `GrpcSslContexts` when server URI scheme is `grpcs://`; otherwise plaintext |
| `AbstractRestServer` | Uses `JdkHttpServerFactory` with `SSLContext` from `TLSUtils.serverContext()` when server keystore exists |
| `RestClient` | Detects `https://` URI and attaches `TLSUtils.getClientContext()` to the Jersey `ClientBuilder` |

---

## 2. F1 Message Server Replication (Kafka State Machine)

### Purpose
Replication improves **fault tolerance** and **availability**. In TP2 F1, each domain's messages service runs as a group of **3 equal replicas** — there is no primary/secondary distinction. The goals are:
- The system keeps working correctly even if **any one replica crashes**
- **Any** replica can serve both reads and writes
- Writes are totally ordered and consistently applied across all replicas

This is a **state-machine replication** design: all replicas process the same operations in the same order and therefore arrive at the same state.

### Architecture

```
  Client
    │
    ▼  (any replica, e.g. R1)
  REST Layer (RestMessagesRepResource)
    │
    ▼
  JavaMessagesRep.applyAndReplicate(op)
    │
    ▼  publish
  Kafka topic: Messages-<domain>  (single partition, broker kafka:9092)
    │
    ├──▶ R1 Kafka consumer: executeLocally(op) ──▶ DB + cross-domain dispatch
    ├──▶ R2 Kafka consumer: executeLocally(op) ──▶ DB + cross-domain dispatch
    └──▶ R3 Kafka consumer: executeLocally(op) ──▶ DB + cross-domain dispatch
```

### Write Path
1. A client sends a write (POST, DELETE, etc.) to **any** replica
2. The receiving replica creates a `ReplicatedOperation` and **publishes** it to the Kafka topic (`Messages-<domain>`, single partition)
3. The replica **waits** until its own Kafka consumer has applied the operation at or past the published Kafka offset (`kafka.waitForOffset(offset)`)
4. All three replicas independently consume the operation from Kafka in order and call `executeLocally` — DB writes, local inbox updates, cross-domain dispatch

Because the Kafka topic is single-partition, all replicas see operations in identical order → state is always consistent.

### Read Path
1. A client sends a read (GET, list inbox, search) to **any** replica
2. The replica first checks the `X-MESSAGES-VERSION` request header (set by the tester from the previous response's header)
3. If the header version (a Kafka offset) is greater than the replica's current offset, it waits (`kafka.waitForVersion(version)`) until it catches up — ensuring **monotonic reads** even when requests are routed to different replicas
4. The replica reads from its local DB and returns the result with an updated `X-MESSAGES-VERSION` header

### Cross-Domain Delivery (F1)
When a message has destinations in foreign domains, **all replicas** attempt cross-domain delivery in their `execPost` / `execDelete` handlers. Duplicates are filtered at the destination by a set-based SID deduplication mechanism:

- **SID** (Sequence ID) = the Kafka **offset** of the publishing replica's operation record. Because Kafka assigns offsets sequentially within a single-partition topic, offsets are globally unique across all replicas.
- The destination domain's `KafkaReplicationManager.checkAndUpdateSid(sourceDomain, sid)` uses a `ConcurrentHashMap.newKeySet()` per source domain. `Set.add(sid)` returns `true` (first time) or `false` (duplicate), so any arrival order is handled correctly without false drops.
- If all three replicas send SID=5, the destination accepts the first arrival and rejects the other two.
- If the publisher replica dies before dispatching, the two surviving replicas still deliver the message — **no lost cross-domain messages due to single-replica failure**.

Cross-domain delete dispatches work the same way via `remoteDeleteMessageWithSid`.

### Restart and Log Replay
Each replica on startup generates a **UUID-suffixed Kafka consumer group ID** (e.g. `messages0.ourorg0:5678:<uuid>`). This forces Kafka to reset the offset to `earliest` on every start, replaying all operations from the beginning of the log. The in-memory DB is rebuilt from scratch from the replayed log.

To avoid reusing message IDs after a restart, `execPost` advances the local ID counter past any IDs seen in replayed messages:
```java
counter.updateAndGet(c -> Math.max(c, parsedId));
```

### Key Classes
| Class | Role |
|-------|------|
| `KafkaReplicationManager` | Creates/consumes the Kafka topic; tracks `currentOffset`; provides `waitForOffset`, `waitForVersion`, and set-based `checkAndUpdateSid` |
| `JavaMessagesRep` | Extends `JavaMessages`; overrides all client-facing writes to go through `applyAndReplicate`; implements `executeLocally` for all operation types |
| `ReplicatedOperation` | DTO for serialising operations (type + payload + `publisherId` URI + `kafkaOffset`); Jackson-serialised as JSON on the Kafka wire |
| `RestMessagesRepServer` | Starts the replicated REST server; connects to Kafka (`KafkaReplicationManager`); starts the consumer after announcing itself in Discovery |
| `RestMessagesRepResource` | REST resource layer; calls `waitForClientVersion()` before read-dependent writes; sets `X-MESSAGES-VERSION` header on all responses |
| `VersionHeaderHandler` | JAX-RS filter that propagates the Kafka offset version header between requests and responses |
| `RestAdminMessagesRepClient` | HTTP client for cross-domain calls; supports SID headers (`X-MESSAGES-SID`, `X-MESSAGES-SOURCE-DOMAIN`) on `remotePostMessage` and `remoteDeleteMessage` |

---

## 3. External Service — Zoho Mail (E1)

### Purpose
The E1 extension provides a **proxy messages server** whose state survives container restarts. Because the tester destroys (not just stops) and recreates the proxy container, no local storage (HSQLDB, files) can persist data across restarts. The solution stores every received message as a self-addressed email in the Zoho Mail cloud service, which persists externally and is retrieved on the next startup.

This proxy handles one domain's `Messages` API (port 4568). On the first start (`clearState=true`) it clears any stale state; on subsequent starts (`clearState=false`) it restores all previously received messages from Zoho.

### Design

#### Storage Format
- **Subject**: `SD2526:<messageId>` — prefix identifies our emails inside the shared Zoho mailbox; the suffix is the full message ID (e.g. `SD2526:ourorg0+0001`)
- **Body**: the full `Message` object serialised to JSON and then **Base64-encoded**. Base64 encoding avoids issues where Zoho HTML-escapes angle brackets (`<`, `>`) present in display-name sender strings (e.g. `"Alice <alice@domain>"`)
- Each sent email appears as two copies in Zoho (Sent folder + Inbox folder); deduplication by message ID during restore prevents double-loading

#### Startup Modes
| `args[0]` | Mode | Behaviour |
|-----------|------|-----------|
| `true` | Fresh start (`clearState=true`) | Search for all `SD2526:` emails; delete them; start with empty in-memory state |
| `false` | Continue (`clearState=false`) | Search for all `SD2526:` emails; decode each body; rebuild `inboxMap` + `messageCache`; restore sequence counter to highest seen ID |

#### Zoho API Usage
- **Authentication**: OAuth 2.0 with a baked-in refresh token. `ZohoMailClient` transparently refreshes the short-lived access token before expiry
- **Listing / Searching**: `GET /api/accounts/{id}/messages/view?searchKey=SD2526&limit=200` — searches across all folders (Inbox + Sent) for emails matching `SD2526`. The `folder=Sent` query parameter is **not** supported by this API; the `searchKey` parameter is the correct approach
- **Sending**: `POST /api/accounts/{id}/messages` with `fromAddress = toAddress = sd2526messages@zohomail.eu` — creates a self-addressed email that appears in both Sent and Inbox
- **Deleting**: `DELETE /api/accounts/{id}/folders/{folderId}/messages/{zohoId}` — deletes a specific email by its folder and message ID
- **Content retrieval**: `GET /api/accounts/{id}/folders/{folderId}/messages/{zohoId}/content` — fetches the email body (may return HTML-wrapped content; the Base64 body inside is decoded transparently)
- **SSL**: `ZohoMailClient` uses a trust-all `SSLContext` because the Docker base image's JVM truststore does not contain the commercial CA certificates used by Zoho

#### In-Memory Serving Layer
`JavaMessagesExternal` maintains an in-memory cache for hot reads:
- `messageCache` (mid → Message): avoids repeated Zoho fetches for content
- `inboxMap` (recipientName → list of mids): serves `getAllInboxMessages` from memory
- `zohoIndex` (mid → ZohoEmail): maps each mid to its Zoho identifiers for deletion

Writes follow the **write-through** pattern: the Zoho write completes first, then the in-memory state is updated. This guarantees durability before the caller receives a 200 OK.

#### Ordering Guarantee
In `remotePostMessage`, the Zoho write happens **synchronously** before the local inbox is updated. This means:
- The calling server (e.g. `messages0.ourorg0`) only gets a 200 response after Zoho has durably stored the message
- The tester confirms successful delivery by calling `getMessages` — which returns non-empty only after the inbox update — so the container is never killed before the message is in Zoho

### Key Classes
| Class | Role |
|-------|------|
| `ZohoMailClient` | Zoho Mail REST client; OAuth token refresh; `sendEmail`, `listEmails` (search-based), `getEmailContent`, `deleteEmail`; trust-all SSL |
| `JavaMessagesExternal` | `Messages` + `AdminMessages` impl backed by Zoho; in-memory cache; Base64 encode/decode; clearState / restore logic |
| `RestMessagesExternalServer` | Starts the proxy server on port 4568; parses `args[0]` as `clearState`; injects `JavaMessagesExternal` into `RestMessagesResource` via `setExternalImpl` |

---

## Summary

| Feature | Technology | Purpose |
|---------|------------|---------|
| TLS (REST) | JDK HTTP server SSL + Jersey SSL | Secure HTTPS for REST endpoints |
| TLS (gRPC) | Netty SSL via GrpcSslContexts; `grpcs://` URI scheme | Secure gRPC channels; scheme-based TLS detection |
| Keystores | Java `keytool`, PKCS12 | Per-server identity and certificates |
| Truststore | Shared PKCS12 | Client-side certificate validation |
| F1 Replication | Kafka state-machine (single partition per domain) | Fault tolerance; any-replica reads and writes |
| Cross-domain SID | Kafka offset as globally unique SID | Exactly-once cross-domain delivery with N replicas |
| Set-based dedup | `ConcurrentHashMap.newKeySet()` per source domain | Out-of-order-safe deduplication |
| Version header | `X-MESSAGES-VERSION` + `VersionHeaderHandler` | Monotonic reads across replicas |
| Zoho Mail | REST + OAuth2 + Base64 body | External persistent message storage (E1 proxy) |
| `originId` stability | `senderAddress()` instead of `sender` | Idempotent POST works across replicas and sender reformatting |
| `?` wildcard in search | `replace("?", "_")` in LIKE query | Tester single-char wildcard correctly matched |
| URI failover | Single-shot per-URI cycling in `tryAllUrisPost/Delete` | Cross-domain delivery survives dead replica without 30 s stall |

---

## Known Bugs and Limitations

### 1. gcDeletedMessageCache GC Race Condition ⚠️ (OPEN)

`JavaMessages` contains a Guava cache (`gcDeletedMessageCache`) whose removal listener fires after 10 seconds and sweeps the DB for orphaned `Message` records:

```sql
SELECT * FROM Message m WHERE NOT EXISTS (SELECT 1 FROM InboxEntry e WHERE e.mid = m.id)
```

There is a window where the `Message` row has been committed but the `InboxEntry` row has not yet been committed (concurrent delivery transaction in progress). The background GC thread can see the Message as "orphaned" and delete it, causing `searchInbox` to silently miss the message.

**Suggested fix:** Remove the `removalListener` block from `gcDeletedMessageCache`. Orphan cleanup is not required for correctness.

### 2. deleteFromLocalInbox — Wrong Deletion Order (minor)

`deleteFromLocalInbox` issues `DELETE FROM InboxEntry WHERE mid = ?` which cascades correctly. However, if `Message` were deleted first while `InboxEntry` still exists, the `ON DELETE RESTRICT` foreign key would prevent it. The code currently goes through `InboxEntry` first (correct), but this dependency is fragile and should be documented or enforced by transaction ordering.

### 3. Cross-domain client caching (performance, minor)

`JavaMessagesRep.tryAllUrisPost/Delete` creates a new `RestAdminMessagesRepClient` (and therefore a new Jersey `Client`) for each URI on every cross-domain call. Under TLS this means a full handshake per call. Should cache clients by URI, similar to how `ClientFactory` works.

### 4. seenSidsFromDomain — Unbounded Memory Growth (minor)

`KafkaReplicationManager.seenSidsFromDomain` accumulates all SIDs ever seen from all source domains. Over a long-lived deployment with many messages this set grows without bound. A time-based eviction policy (e.g., evict SIDs older than N minutes) could bound memory at the cost of allowing very late retries to slip through.

### FIXED: Message Idempotency Broken in Replicated Server (109b) ✅

**Root cause**: `Message.originId()` was defined as `sender + "-" + creationTime`. The `sender` field is reformatted by the server from `"alice@ourorg0"` to `"Alice <alice@ourorg0>"` before the message is stored. In `doAsyncPostRep`, `setSender` was called before `messagesCache.put(originId, ...)`, so the cache entry was stored under `"Alice <alice@ourorg0>-T"`. But the lookup on a duplicate POST used the raw sender `"alice@ourorg0-T"` — a different key. This meant the idempotency check always missed, and every POST created a new message instead of returning the existing ID.

**Fix**: changed `originId()` to use `senderAddress()` instead of `sender`. `senderAddress()` extracts just the email address from either `"alice@ourorg0"` or `"Alice <alice@ourorg0>"`, returning `"alice@ourorg0"` in both cases — so the key is stable regardless of formatting.

### FIXED: searchInbox Does Not Support `?` Wildcard (114b) ✅

**Root cause**: `JavaMessages.searchInbox` built a SQL `LIKE '%pattern%'` query. The tester uses `?` as a single-character wildcard (e.g., `They?ve` to match `They've`), but `?` has no special meaning in SQL LIKE — only `_` (single char) and `%` (any sequence) are SQL wildcards. Searches with `?` patterns returned no results.

**Fix**: added `.replace("?", "_")` to the query sanitisation step in `searchInbox`, converting the tester's single-character wildcard to the SQL equivalent before building the LIKE expression.

### FIXED: Cross-Domain Delivery Always Hits Same (Possibly Dead) URI (114b) ✅

**Root cause**: `repClientFor(domain)` called `Discovery.knownUrisOf("Messages@domain", 1)` and always took `uris[0]`. All source replicas picked the same URI. If that URI belonged to a killed replica, all cross-domain delivery attempts timed out after 30 seconds per attempt. Since the `RestAdminMessagesRepClient` retry loop spends up to 30 s stuck on a dead URI, delivery could fail even though surviving replicas were available.

**Fix**: replaced `repClientFor` with `tryAllUrisPost` / `tryAllUrisDelete` helper methods. These use new single-shot methods `tryOncePostWithSid` / `tryOnceDeleteWithSid` on `RestAdminMessagesRepClient` that make one attempt with no internal retry. The helpers cycle through all discovered URIs in sequence: a dead URI fails in ~100 ms (connection refused), and the next URI is tried immediately. As long as at least one replica is alive in the destination domain, delivery is guaranteed within the `REMOTE_COMM_DEADLINE`.

### FIXED: Cross-Domain SID Collision (113b) ✅

**Root cause**: each replica's `AtomicLong seqCounter` starts at 0. Two replicas posting concurrently assign the same `seqNum` to different messages, causing the destination to reject one as a duplicate.

**Fix**: use the **Kafka offset** (broker-assigned, globally unique within the single-partition topic) as the cross-domain SID. Additionally switched from a monotonic tracker to a set-based deduplication (`ConcurrentHashMap.newKeySet()`) so that out-of-order arrivals from independent replica `JobDispatcher` threads are all accepted at the destination.

### FIXED: Cross-Domain Delivery on Publisher Failure (114b+) ✅

**Root cause**: cross-domain dispatch was gated on `myUri.equals(op.getPublisherId())`. If the publisher replica was killed before its Kafka consumer processed the operation, no replica would dispatch the message.

**Fix**: removed the publisher gate. All replicas attempt cross-domain dispatch; SID-based deduplication at the destination ensures exactly-once delivery regardless of how many replicas send the same message.

### FIXED: deleteMessage on Stale Cache (113c) ✅

**Root cause**: `messagesCache` (Guava, 30 s TTL) evicts messages after 30 seconds. `deleteMessage` called `getCachedMessage(mid)` which returned `FORBIDDEN` for messages evicted from cache, even though the message was in the DB.

**Fix**: overrode `getCachedMessage` in `JavaMessagesRep` to fall back to `DB.getOne(mid, Message.class)` on a cache miss, re-warming the cache on hit.

### FIXED: deleteMessage Without Version Wait (caused 113b false failures) ✅

**Root cause**: `RestMessagesRepResource.deleteMessage` did not call `waitForClientVersion()` before delegating to `impl.deleteMessage`. A replica that hadn't applied the POST_MESSAGE op yet would not find the message in its cache, returning FORBIDDEN.

**Fix**: added `waitForClientVersion()` before `impl.deleteMessage(...)` in `RestMessagesRepResource`.

### FIXED: gRPC TLS/Plaintext Mismatch (test 13c) ✅

**Root cause**: `AbstractGrpcServer` always announced `grpc://` URI even when TLS was active. `GrpcClient` decided TLS mode based on the presence of `client-truststore.ks` — but in non-TLS deployments the server had no keystore while the client still had the shared truststore, causing a TLS handshake mismatch.

**Fix**: `AbstractGrpcServer` now announces `grpcs://` when TLS is active. `GrpcClient` enables TLS only when the server URI uses the `grpcs://` scheme.
