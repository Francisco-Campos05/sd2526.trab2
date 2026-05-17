# Theoretical Overview — SD TP2 Extensions

This document explains the two major features added on top of the TP1 base: **TLS Security** and **F2b Replication**.

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
- **Server side**: `NettyServerBuilder` receives an `SslContext` built with `GrpcSslContexts.configure(SslContextBuilder.forServer(kmf))`, where `kmf` is a `KeyManagerFactory` loaded from the server's `.ks` file.
- **Client side**: `NettyChannelBuilder` receives an `SslContext` built with `GrpcSslContexts.forClient().trustManager(tmf)`, where `tmf` is a `TrustManagerFactory` loaded from the shared truststore.

An important detail: gRPC server URIs keep the `grpc://` scheme (not `grpcs://`) because the tester's Discovery service only accepts `grpc:` as a valid scheme. TLS is activated based on the **presence of the keystore file**, not the URI scheme.

#### Key Classes Modified
| Class | Role |
|-------|------|
| `TLSUtils` | Loads keystores/truststores; provides `serverContext`, `getClientContext`, `keyManagerFactory`, `getTrustManagerFactory`, `keystoreExists` |
| `AbstractGrpcServer` | Uses `NettyServerBuilder` + `GrpcSslContexts` when server keystore exists |
| `GrpcClient` | Uses `NettyChannelBuilder` + `GrpcSslContexts` when server keystore exists on the LOCAL host (guards against TLS client → plaintext server mismatch) |
| `AbstractRestServer` | Uses `JdkHttpServerFactory` with `SSLContext` from `TLSUtils.serverContext()` when server keystore exists |
| `RestClient` | Detects `https://` URI and attaches `TLSUtils.getClientContext()` to the Jersey `ClientBuilder` |

#### Important TLS Design Decisions
- **TLS guard uses SERVER keystore, not truststore.** `GrpcClient` checks `TLSUtils.keystoreExists(IP.hostname())` before enabling TLS. This prevents a TLS client from connecting to a plaintext server: if the local container has no server keystore, it is a non-TLS deployment and all outgoing connections must also be plaintext.
- **`client-truststore.ks` is always present** inside Docker (generated at image build time by `generate-keystores.sh`). Therefore the truststore alone cannot be used as the TLS mode indicator.
- **gRPC URI scheme stays `grpc://`** even when TLS is active — the Discovery/tester infrastructure only recognises `grpc:` as a valid scheme and would reject `grpcs://`.

---

## 2. F2b Message Server Replication

### Purpose
Replication improves **fault tolerance** and **availability**. In TP2, each domain's messages service runs as a group of **3 replicas** (replica 0 = primary, replicas 1 and 2 = secondaries). The goal is:
- The system keeps working correctly even if **one replica crashes**
- Clients can read from any replica (reads scale horizontally)
- Writes always go through the primary (consistency is maintained)

The "F2b" model means the system tolerates **up to 1 Byzantine or crash failure** among 3 replicas (`f = 1`, `n = 2f + 1 = 3`).

### How It Works

#### Roles: Primary and Secondary
- **Primary (replica 0)**: receives all write operations (post message, delete message). After applying the write locally, it **forwards** the operation to all secondaries and waits for their acknowledgement before returning success to the client.
- **Secondary (replicas 1 and 2)**: receive forwarded write operations from the primary and apply them. They can serve **read operations** directly (get message, list inbox, search).

#### Write Path
```
Client
  └─▶ Primary (replica 0)
         ├─▶ Apply write locally
         ├─▶ Forward to Secondary 1  ──▶ Apply + ACK
         ├─▶ Forward to Secondary 2  ──▶ Apply + ACK
         └─▶ Return success to client
```

#### Read Path
```
Client
  └─▶ Any replica (0, 1, or 2)
         └─▶ Read locally and return result
```

#### Failure Handling
- If a **secondary** is down: the primary continues, writing only to the surviving secondary. The system remains consistent, just with reduced redundancy.
- If the **primary** is down: secondaries detect the absence of the primary (via heartbeat or failed request) and one of them is **elected** as the new primary. Writes resume through the new primary.
- When a failed replica **restarts**: it contacts the primary to catch up on missed operations (state transfer / log replay) before rejoining the group.

#### Discovery and Routing
The tester routes write requests to the primary and can rotate reads across all replicas. Each replica announces itself to the Discovery service with its role. The `Clients` factory resolves the correct endpoint depending on whether the operation is a read or a write.

#### Key Classes Involved
| Class | Role |
|-------|------|
| `JavaMessagesRep` | Extends `JavaMessages`; overrides all write operations to go through `applyAndReplicate` |
| `ReplicationManager` | Tracks sequence numbers, secondary URIs, primary URI, and per-domain SIDs for deduplication |
| `ReplicatedOperation` | DTO for serialising operations over the wire (POST_MESSAGE, DELETE_MESSAGE, REMOTE_POST, REMOTE_DELETE, DELETE_INBOX, REMOVE_INBOX_ENTRY) |
| `RestMessagesRepServer` | Starts the replicated REST server; on startup announces as `MessagesPrimary@domain` (primary) or discovers and registers with the primary (secondary) |
| `RestMessagesRepResource` | REST resource layer for the replicated server; enforces `requirePrimary()` on writes and `checkVersionOrRedirect()` on reads |
| `RestAdminMessagesRepClient` | HTTP client for intra-replica communication; supports SID headers (`X-MESSAGES-SID`, `X-MESSAGES-SOURCE-DOMAIN`) and the `applyReplicatedOp` endpoint |
| `VersionHeaderHandler` | JAX-RS filter that reads/writes the `X-MESSAGES-VERSION` header for read-your-writes consistency |
| `AdminMessages` interface | Defines `remotePostMessage` / `remoteDeleteMessage` — used for both cross-domain and replication forwarding |

---

## Summary

| Feature | Technology | Purpose |
|---------|------------|---------|
| TLS (REST) | JDK HTTP server SSL + Jersey SSL | Secure HTTPS for REST endpoints |
| TLS (gRPC) | Netty SSL via GrpcSslContexts | Secure gRPC channels |
| Keystores | Java `keytool`, PKCS12 | Per-server identity and certificates |
| Truststore | Shared PKCS12 | Client-side certificate validation |
| Replication | Primary/secondary model | Fault tolerance, availability |
| Write forwarding | `applyAndReplicate` + `RestAdminMessagesRepClient` | Consistency across replicas |
| Sequence numbers | `ReplicationManager.seqNum` + SID headers | Cross-domain deduplication and ordering |
| Version header | `X-MESSAGES-VERSION` + `VersionHeaderHandler` | Read-your-writes consistency on secondaries |

---

## Known Bugs and Limitations

### 1. gcDeletedMessageCache GC Race Condition ⚠️ (OPEN — likely cause of test 105e failure)

`JavaMessages` contains a Guava cache (`gcDeletedMessageCache`) whose removal listener fires after 10 seconds and sweeps the DB for orphaned `Message` records:

```sql
SELECT * FROM Message m WHERE NOT EXISTS (SELECT 1 FROM InboxEntry e WHERE e.mid = m.id)
```

With TLS, remote message delivery takes longer. There is a window where the `Message` row has been committed but the `InboxEntry` row has not yet been committed (if the delivery transaction is still in progress on another thread). The background GC thread can see the Message as "orphaned" and delete it. This causes `searchInbox` (which does an INNER JOIN on Message+InboxEntry) to silently miss the message.

**Fix:** Remove the `removalListener` block from `gcDeletedMessageCache`. Orphan cleanup is not required for correctness.

### 2. deleteFromLocalInbox — Wrong Deletion Order ⚠️ (secondary)

`deleteFromLocalInbox` deletes the `Message` record before deleting its `InboxEntry` rows. The `InboxEntry` table has a `FOREIGN KEY … ON DELETE RESTRICT` constraint, so the Message delete fails silently when InboxEntries still exist. HSQLDB may or may not enforce the constraint strictly, but the code order is logically wrong. InboxEntries should be deleted first.

### 3. repClientFor() — No Client Caching (performance)

`JavaMessagesRep.repClientFor(domain)` creates a new `RestAdminMessagesRepClient` (and therefore a new Jersey `Client`) on every replication call. Under TLS this means a full handshake per call. Should cache by URI, similar to how `ClientFactory` works.

### 4. checkAndUpdateSid — Out-of-Order Operation Drop (edge case)

`ReplicationManager.checkAndUpdateSid` rejects any SID ≤ the last seen SID from a source domain. If retries cause an older operation to arrive after a newer one, it is silently dropped. The single-threaded `JobDispatcher` per domain normally prevents reordering, but network retries can violate this. Not currently causing test failures, but a latent correctness risk.
