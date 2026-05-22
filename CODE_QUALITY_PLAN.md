# Code Quality and Clarity Upgrade Plan

## Context

All 33 tests (101a–116b) now pass. The codebase was built incrementally across many
sessions to fix specific test failures, which means a lot of duplication, inconsistent
style, and parallel implementations have accumulated. With the correctness phase done,
the goal is to make this submission-quality: clearer, shorter, more consistent —
**without changing behaviour**.

**Hard constraint**: every change must preserve the 101a–116b pass. Commit after
each phase so any regression can be bisected easily.

---

## Findings (current state)

### A. Significant duplication

1. **Three single-threaded executor patterns**: `JavaMessages.JobDispatcher` (inner
   class), `JavaMessagesExternal.domainExecutors` + `submitDomainJob`, and
   `JavaMessagesExternal.zohoWriter` — all spin up `newSingleThreadExecutor` with
   `setUncaughtExceptionHandler(printStackTrace)`. Same idea, three implementations.

2. **`JavaMessages.doAsyncPost` ↔ `JavaMessagesRep.doAsyncPostRep`** — same ~25-line
   shape: idempotency check via `getCachedMessage(originId)`, ID assignment, sender
   reformat, local/remote split, dispatch. Rep adds Kafka publish; otherwise identical.

3. **`RestAdminMessagesRepClient.remotePostMessageWithSid` ↔ `tryOncePostWithSid`** —
   identical request building; only difference is whether `super.reTry(...)` wraps it.
   Same story for `…DeleteWithSid` ↔ `tryOnceDeleteWithSid`. Four near-identical methods.

4. **`JavaMessagesRep.tryAllUrisPost` ↔ `tryAllUrisDelete`** — same loop, same
   Discovery call, same sleep; only the per-URI call differs.

5. **`RestMessagesClient`, `RestUsersClient`, `RestAdminMessagesClient`,
   `RestAdminMessagesRepClient`** — every method follows
   `super.reTry(() -> doXxx(...))` with a private `doXxx` building
   `target.request()...`. Could be one line per method with a small helper.

6. **`JavaMessages.getLocalRecipientAddresses/getRemoteRecipientAddresses` vs
   `JavaMessagesExternal.getLocalRecipients/getRemoteRecipients`** — identical logic,
   different names, two separate classes.

7. **`JavaMessages.getUser` vs `JavaMessagesExternal.getUser`** — line-for-line
   identical; Zoho impl re-implements it because it doesn't extend `JavaMessages`.

8. **Counter-advance logic** — in `JavaMessagesRep.execPost` and
   `JavaMessagesExternal.updateCounter`, with subtly different parsing (Rep uses
   `lastIndexOf('+')`, External splits on `\\.`).

### B. Smells / design issues

9. **`RestMessagesResource.externalImpl` static injection hook** — global mutable
   state used as a poor-man's DI. The same class has both `final boolean isGateway`
   *and* the static hook, plus a per-instance `impl` field. Resolution order is
   confusing.

10. **`((AdminMessages)impl()).remotePostMessage(...)`** — unsafe cast in
    `RestMessagesResource`. In the gateway path, `impl()` returns a `MessagesClient`
    which is NOT an `AdminMessages` → ClassCastException waiting to happen. Today
    it works only because admin endpoints aren't hit on the gateway path.

11. **`ReplicatedOperation`** has 6 static factory methods, each setting a different
    subset of 12 fields. Some fields (e.g. `destinations` vs `remoteDestinations`)
    overlap semantically but are stored differently per op type. Hard to read.

12. **`System.out.println("Local Recipients:" …)`** in `JavaMessages.doAsyncPost`
    (lines 284–285) — leftover debug print, should use `Log`.

13. **`import java.util.*;` and `import java.util.concurrent.*;`** in
    `JavaMessagesExternal` — wildcard imports; every other file uses explicit imports.

14. **Indentation inconsistency** — `JavaMessages.java` uses tabs; `JavaMessagesRep`,
    `JavaMessagesExternal`, etc. use 4 spaces. Same package.

15. **`messagesCache` used as two caches in one** — keyed by `originId` (idempotency)
    AND by `mid` (delete lookup). No comment; readers must infer.

16. **`gcDeletedMessageCache` removal-listener GC race** — already documented as
    open bug in THEORETICAL_OVERVIEW. The suggested fix is drop the listener.

17. **`DB.reTry` retries by checking `f.get() != null`** — but `Result.error(...)`
    is non-null too, so this isn't really retrying on failure. Effectively dead.

18. **`JavaMessagesExternal.parseBody`** — three nested fallbacks with silently
    swallowed exceptions. Necessary for backward-compat with HTML-wrapped Zoho
    bodies, but control flow is hard to follow.

19. **`RestGatewayServer` has commented-out registration code** (lines 20–21).

20. **`final String finalMid = mid` in JavaMessagesExternal:243** — redundant,
    `mid` is already effectively final.

---

## Refactor plan (phased, low → high risk)

### Phase 1 — Cleanup (zero behavioural change, no test risk)

**Files**: `JavaMessages.java`, `JavaMessagesExternal.java`, `RestGatewayServer.java`

- Replace `System.out.println` (JavaMessages:284-285) with `Log.info(...)`
- Expand wildcard imports in `JavaMessagesExternal.java` to explicit imports
- Normalise indentation: convert all impl/server files to 4-space (dominant style)
- Delete commented-out code in `RestGatewayServer.java` (lines 20–21)
- Remove `final String finalMid = mid` redundancy in `JavaMessagesExternal:243`
- Add doc comment to `messagesCache` in `JavaMessages` explaining dual keying

**Verification**: `mvn package` + `docker build`, run `-test 116b` (fastest end-to-end
test, ~5 min). Commit as "Phase 1: cleanup".

---

### Phase 2 — Extract reusable utilities (low risk)

**New file**: `src/sd2526/trab/impl/utils/PerDomainExecutor.java`

Pull the per-domain single-threaded executor pattern into one class with
`submit(String key, Runnable job)`. Replaces:
- `JavaMessages.JobDispatcher` (inner class)
- `JavaMessagesExternal.submitDomainJob` + `domainExecutors`

Both classes hold a `PerDomainExecutor jobs` field instead.

**New method**: `Message.localAndRemoteSplit()` — returns a record/pair of
`(List<String> local, Set<String> remote)`. Replaces 4 copies of the streaming
pattern.

**Verification**: `mvn package` + `docker build`, run `-test 116b`. Commit.

---

### Phase 3 — Collapse RestAdminMessagesRepClient duplication

**File**: `RestAdminMessagesRepClient.java`

Refactor `remotePostMessageWithSid` / `tryOncePostWithSid` so both delegate to a
single private `doPost(...)`:

```java
private Result<Void> doPost(Message msg, Long sid, String src) {
    var req = target.path(RestAdminMessages.ADMIN).request();
    if (sid != null) req = req.header("X-MESSAGES-SID", sid)
                              .header("X-MESSAGES-SOURCE-DOMAIN", src);
    return super.toJavaResult(req.post(Entity.entity(msg, MediaType.APPLICATION_JSON)));
}
public Result<Void> remotePostMessageWithSid(...)  { return super.reTry(() -> doPost(...)); }
public Result<Void> tryOncePostWithSid(...)        {
    try { return doPost(...); }
    catch (Exception e) { return Result.error(TIMEOUT); }
}
```

Same shape for delete. Cuts ~30 lines, removes the chance of the two variants
drifting apart.

**Verification**: same as Phase 1. Commit.

---

### Phase 4 — Unify `tryAllUris` helpers

**File**: `JavaMessagesRep.java`

Replace `tryAllUrisPost` and `tryAllUrisDelete` with one generic helper:

```java
private Result<Void> tryAllUris(String domain,
        Function<RestAdminMessagesRepClient, Result<Void>> call) {
    long deadline = System.currentTimeMillis() + REMOTE_COMM_DEADLINE;
    Result<Void> res = error(ErrorCode.TIMEOUT);
    while (System.currentTimeMillis() < deadline) {
        var uris = Discovery.getInstance().knownUrisOf("Messages@" + domain, 1);
        for (var uri : uris) {
            res = call.apply(new RestAdminMessagesRepClient(uri.toString()));
            if (res.isOK()) return res;
        }
        Sleep.ms(500);
    }
    return res;
}
```

Call sites become one-liners. Commit.

---

### Phase 5 — Fix the unsafe `AdminMessages` cast (correctness)

**File**: `RestMessagesResource.java`

The admin endpoints cast `impl()` to `AdminMessages`. In the gateway path, `impl()`
is a `MessagesClient` which doesn't implement `AdminMessages` → ClassCastException
if those endpoints were ever called via the gateway.

**Fix options** (pick whichever is shortest after exploring):
- (a) Add `adminImpl()` that returns `NOT_IMPLEMENTED` on the gateway path
- (b) Split into two resource classes — only non-gateway servers register the
  admin one

**Risk**: medium — touches wiring. Verify with `-test 108a` (gateway path) and
`-test 113b` (cross-domain admin path) before committing.

---

### Phase 6 — Lift shared helpers into `JavaBaseService` (medium risk)

The Zoho impl can't extend `JavaMessages` (different storage model), but the
helpers it duplicates (`getUser`, `getLocalRecipients`, `getRemoteRecipients`,
counter-advance) can be promoted to `JavaBaseService` as `protected` so both
`JavaMessages` and `JavaMessagesExternal` share them.

**Risk**: medium — `JavaMessages` is older and may have subtly different behaviour.
Diff carefully before promoting. Verify with `-test 108b` (E1 path) and `-test 113b`
(F1 path). Commit.

---

### Phase 7 — Address documented open bugs (correctness)

From THEORETICAL_OVERVIEW.md → Known Bugs:

- **Drop `gcDeletedMessageCache.removalListener`** — orphan cleanup isn't required
  for correctness and creates a GC race. Single-line removal.
- **Bound `KafkaReplicationManager.seenSidsFromDomain`** — add time-based eviction
  so memory doesn't grow unbounded. *Defer if time-poor — it's a long-running
  concern, not a test-relevant one.*

**Risk**: low for the removal-listener (it makes code *less* aggressive).

---

### Phase 8 — Documentation polish

- Update THEORETICAL_OVERVIEW.md: remove the bugs that were fixed in earlier phases
  from the "Known Bugs" section, update the summary table
- Add a brief class-level Javadoc to each top-level class explaining its role
  (rich in `JavaMessagesExternal`, absent in `JavaMessages`, `JavaMessagesRep`,
  `KafkaReplicationManager`)

---

## Verification strategy

After **each** phase:
1. `mvn clean package -q -DskipTests` — must compile clean
2. `docker build -t sd2526-trab2 .` — image rebuilds
3. Spot-check `-test 116b` (~5 min, fastest test that exercises both E1+F1 paths
   indirectly)
4. Commit with `"Phase N: <summary>"`

After Phases 5 and 6 (the medium-risk ones), additionally run `-test 109a` and
`-test 108b` to cover F1 and E1 paths.

Final step:
- Full regression from `-test 101a` (~40 min) to confirm nothing regressed,
  then a final consolidation commit + push.

---

## Out of scope

- Generated gRPC classes (`grpc/generated_java/*`)
- TLS / certificate handling (works, not worth touching)
- Discovery protocol (works, isolated)
- ZohoMailClient credentials & trust-all SSL (intentional for assignment)
- Any algorithmic / behavioural change beyond the items listed above
- Performance optimisation beyond removing dead code
