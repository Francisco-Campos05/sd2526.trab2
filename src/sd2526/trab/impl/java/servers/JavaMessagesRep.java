package sd2526.trab.impl.java.servers;

import static sd2526.trab.api.java.Result.error;
import static sd2526.trab.api.java.Result.ok;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import sd2526.trab.api.Message;
import sd2526.trab.api.User;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Result.ErrorCode;
import sd2526.trab.impl.db.DB;
import sd2526.trab.impl.discovery.Discovery;
import sd2526.trab.impl.rest.clients.RestAdminMessagesRepClient;

public class JavaMessagesRep extends JavaMessages {

    private static final Logger Log = Logger.getLogger(JavaMessagesRep.class.getName());

    private final KafkaReplicationManager kafka;
    private final String myUri;

    public JavaMessagesRep(KafkaReplicationManager kafka, String myUri) {
        super();
        this.kafka = kafka;
        this.myUri = myUri;
    }

    public KafkaReplicationManager getKafka() { return kafka; }

    // ---- Client-facing write overrides ----

    @Override
    public Result<String> postMessage(String pwd, Message msg) {
        Log.info(() -> "postMessage (F1): sender=" + msg.getSender());
        var userResult = getUser(msg.getSender(), pwd);
        if (!userResult.isOK()) return error(userResult.error());
        return doAsyncPostRep(userResult.value(), msg);
    }

    @Override
    public Result<Void> deleteMessage(String name, String mid, String pwd) {
        Log.info(() -> "deleteMessage (F1): name=" + name + " mid=" + mid);
        var userResult = getUser(name, pwd);
        if (!userResult.isOK()) return error(userResult.error());

        var msgResult = getCachedMessage(mid);
        if (!msgResult.isOK()) return error(ErrorCode.FORBIDDEN);
        var msg = msgResult.value();
        if (!name.equals(getName(msg.senderAddress()))) return error(ErrorCode.FORBIDDEN);

        var op = ReplicatedOperation.deleteMessage(kafka.nextSeqNum(), mid, new HashSet<>(msg.getDestination()));
        op.setPublisherId(myUri);
        return applyAndReplicate(op);
        // Cross-domain dispatch happens in execDelete() using the Kafka offset as SID
    }

    @Override
    public Result<Void> remotePostMessage(Message msg) {
        return remotePostWithSid(msg, null, null);
    }

    public Result<Void> remotePostWithSid(Message msg, Long sid, String sourceDomain) {
        var localAddresses = msg.getDestination().stream().filter(super::isLocalAddress).toList();
        var knownLocal = computeKnownLocal(localAddresses);
        long seq = kafka.nextSeqNum();
        var op = ReplicatedOperation.remotePost(seq,
                sid != null ? sid : seq, sourceDomain, msg, knownLocal);
        op.setPublisherId(myUri);
        return applyAndReplicate(op);
    }

    @Override
    public Result<Void> remoteDeleteMessage(String mid) {
        return remoteDeleteWithSid(mid, null, null);
    }

    public Result<Void> remoteDeleteWithSid(String mid, Long sid, String sourceDomain) {
        long seq = kafka.nextSeqNum();
        var op = ReplicatedOperation.remoteDelete(seq,
                sid != null ? sid : seq, sourceDomain, mid);
        op.setPublisherId(myUri);
        return applyAndReplicate(op);
    }

    @Override
    public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
        Log.info(() -> "removeInboxMessage (F1): name=" + name + " mid=" + mid);
        var userResult = getUser(name, pwd);
        if (!userResult.isOK()) return error(userResult.error());
        var op = ReplicatedOperation.removeInboxEntry(kafka.nextSeqNum(), mid, name);
        op.setPublisherId(myUri);
        return applyAndReplicate(op);
    }

    @Override
    public Result<Void> remoteDeleteUserInbox(String name) {
        Log.info(() -> "remoteDeleteUserInbox (F1): name=" + name);
        var op = ReplicatedOperation.deleteInbox(kafka.nextSeqNum(), name);
        op.setPublisherId(myUri);
        return applyAndReplicate(op);
    }

    // ---- Cache with DB fallback ----

    /**
     * Override base-class cache lookup to also check the DB.
     * The base cache expires after 30 s; for deleteMessage the message may have aged out
     * even though it still exists in the DB (it was persisted there at delivery time).
     */
    @Override
    protected Result<Message> getCachedMessage(String mid) {
        var cached = super.getCachedMessage(mid);
        if (cached.isOK()) return cached;
        // Cache miss — try the DB (message may have been evicted from the 30-s cache)
        var dbResult = DB.getOne(mid, Message.class);
        if (dbResult.isOK()) {
            messagesCache.put(mid, dbResult.value()); // re-warm the cache
        }
        return dbResult;
    }

    // ---- Core Kafka dispatch ----

    private Result<Void> applyAndReplicate(ReplicatedOperation op) {
        try {
            long offset = kafka.publish(op);
            kafka.waitForOffset(offset);
            return ok();
        } catch (Exception e) {
            Log.warning("Kafka replication failed: " + e.getMessage());
            return error(ErrorCode.INTERNAL_ERROR);
        }
    }

    // ---- Kafka consumer callback ----

    public void executeLocally(ReplicatedOperation op) {
        try {
            switch (op.getType()) {
                case POST_MESSAGE       -> execPost(op);
                case DELETE_MESSAGE     -> execDelete(op);
                case REMOTE_POST        -> execRemotePost(op);
                case REMOTE_DELETE      -> execRemoteDelete(op);
                case DELETE_INBOX       -> execDeleteInbox(op);
                case REMOVE_INBOX_ENTRY -> execRemoveInboxEntry(op);
            }
        } catch (Exception e) {
            Log.warning("executeLocally error for " + op.getType() + ": " + e.getMessage());
        }
    }

    // ---- Local executors (called from Kafka consumer thread) ----

    private void execPost(ReplicatedOperation op) {
        var msg = op.getMessage();
        // Advance counter past replayed IDs so restarts don't reuse IDs (format: domain+NNNN)
        var mid = msg.getId();
        int plus = mid.lastIndexOf('+');
        if (plus >= 0) {
            try { counter.updateAndGet(c -> Math.max(c, Long.parseLong(mid.substring(plus + 1)))); }
            catch (NumberFormatException ignored) {}
        }
        messagesCache.put(msg.originId(), new Message(msg));
        messagesCache.put(msg.getId(), msg);

        if (!op.getLocalRecipients().isEmpty())
            persistToInboxSafe(op.getLocalRecipients(), msg);

        // All replicas attempt cross-domain dispatch — SID dedup at destination ensures exactly-once.
        // Using all replicas (not just publisher) ensures delivery survives publisher failure (114b+).
        if (!op.getRemoteDestinations().isEmpty()) {
            long mySid = op.getKafkaOffset(); // Kafka offset is globally unique across all replicas
            op.getRemoteDestinations().stream()
                .collect(Collectors.groupingBy(super::getDomain, Collectors.toSet()))
                .forEach((domain, addrs) ->
                    jobs.submit(domain, () -> {
                        var res = tryAllUrisPost(domain, msg, mySid, THIS_DOMAIN);
                        if (res.error() == ErrorCode.TIMEOUT)
                            addrs.forEach(a ->
                                persistToInboxSafe(List.of(msg.senderAddress()),
                                    msg.cloneWithTimeout(a)));
                    }));
        }
    }

    private void execDelete(ReplicatedOperation op) {
        // Delete from local inbox on every replica
        op.getDestinations().stream()
            .map(r -> r.split("@")[1])
            .filter(THIS_DOMAIN::equals)
            .forEach(__ -> deleteFromLocalInbox(op.getMessageId()));

        // All replicas attempt cross-domain delete dispatch — SID dedup at destination deduplicates.
        long mySid = op.getKafkaOffset();
        op.getDestinations().stream()
            .map(r -> r.split("@")[1])
            .filter(d -> !d.equals(THIS_DOMAIN))
            .distinct()
            .forEach(domain -> jobs.submit(domain, () ->
                tryAllUrisDelete(domain, op.getMessageId(), mySid, THIS_DOMAIN)));
    }

    private void execRemotePost(ReplicatedOperation op) {
        // SID dedup — all replicas run this, so they all deduplicate consistently
        if (op.getSourceDomain() != null && op.getSid() != null
                && !kafka.checkAndUpdateSid(op.getSourceDomain(), op.getSid())) {
            Log.info("Duplicate remotePost from " + op.getSourceDomain() + " sid=" + op.getSid());
            return;
        }

        var msg = op.getMessage();
        var known = op.getLocalRecipients() != null ? op.getLocalRecipients() : List.<String>of();
        if (!known.isEmpty()) persistToInboxSafe(known, msg);

        // Send error back to sender for unknown local recipients (publisher only to avoid dups)
        if (myUri.equals(op.getPublisherId())) {
            var allLocal = msg.getDestination().stream().filter(super::isLocalAddress).toList();
            allLocal.stream().filter(a -> !known.contains(a)).forEach(addr -> {
                var errMsg = msg.cloneWithUserNotFound(addr);
                var senderDomain = getDomain(msg.senderAddress());
                if (isLocalDomain(senderDomain))
                    persistToInboxSafe(List.of(msg.senderAddress()), errMsg);
                else
                    jobs.submit(senderDomain, () ->
                        tryAllUrisPost(senderDomain, errMsg, null, null));
            });
        }
    }

    private void execRemoteDelete(ReplicatedOperation op) {
        if (op.getSourceDomain() != null && op.getSid() != null
                && !kafka.checkAndUpdateSid(op.getSourceDomain(), op.getSid())) {
            Log.info("Duplicate remoteDelete from " + op.getSourceDomain() + " sid=" + op.getSid());
            return;
        }
        deleteFromLocalInbox(op.getMessageId());
    }

    private void execRemoveInboxEntry(ReplicatedOperation op) {
        var entry = new InboxEntry(op.getMessageId(), op.getUserName());
        DB.deleteOne(entry).mapToVoid()
            .then(() -> gcDeletedMessageCache.put(op.getMessageId(), op.getMessageId()));
    }

    private void execDeleteInbox(ReplicatedOperation op) {
        var sql = "SELECT * FROM InboxEntry e WHERE e.recipient = '%s'".formatted(op.getUserName());
        DB.transaction(h -> h.select(sql, InboxEntry.class).thenWith(entries -> {
            h.deleteMany(entries);
            entries.forEach(e -> gcDeletedMessageCache.put(e.mid, e.mid));
            return ok();
        }));
    }

    // ---- Post from local client ----

    private Result<String> doAsyncPostRep(User sender, Message msg) {
        return getCachedMessage(msg.originId()).mapValue(Message::getId).orElse(() -> {
            msg.setId("%s+%04d".formatted(THIS_DOMAIN, counter.incrementAndGet()));
            msg.setSender("%s <%s@%s>".formatted(
                    sender.getDisplayName(), sender.getName(), sender.getDomain()));

            // Pre-cache for idempotency guard (execPost will re-put after Kafka delivery)
            messagesCache.put(msg.originId(), new Message(msg));

            var local  = msg.getDestination().stream().filter(super::isLocalAddress).toList();
            var remote = msg.getDestination().stream()
                    .filter(a -> !isLocalAddress(a)).collect(Collectors.toSet());
            var knownLocal = computeKnownLocal(local);

            var op = ReplicatedOperation.postMessage(
                    kafka.nextSeqNum(), new Message(msg), knownLocal, remote);
            op.setPublisherId(myUri);

            return applyAndReplicate(op)
                    .mapValue(__ -> msg.getId())
                    .orElse(() -> ok(msg.getId()));
        });
    }

    // ---- Helpers ----

    private List<String> computeKnownLocal(Collection<String> addresses) {
        if (addresses.isEmpty()) return List.of();
        var unknownResult = checkUsers(addresses);
        if (!unknownResult.isOK()) return List.of();
        var unknown = unknownResult.value();
        return addresses.stream().filter(a -> !unknown.contains(a)).collect(Collectors.toList());
    }

    private void persistToInboxSafe(Collection<String> recipients, Message msg) {
        try {
            DB.transaction(h -> {
                h.persistOne(msg);
                for (var r : recipients)
                    h.persistOne(new InboxEntry(msg.getId(), getName(r)));
                return ok();
            });
        } catch (Exception e) {
            // Idempotent: ignore duplicate-key errors on Kafka replay after crash
            Log.fine("persistToInboxSafe: ignoring duplicate for " + msg.getId());
        }
    }

    private void deleteFromLocalInbox(String mid) {
        var sql = "SELECT * FROM InboxEntry e WHERE e.mid = '%s'".formatted(mid);
        DB.transaction(h -> h.select(sql, InboxEntry.class).thenWith(h::deleteMany));
    }

    /**
     * Try every known URI for the destination domain in sequence, using a single-shot
     * attempt per URI (no internal retry). Cycles through all URIs until one succeeds
     * or the REMOTE_COMM_DEADLINE is exhausted. This ensures delivery survives a dead
     * replica without spending 30 s stuck on it.
     */
    private Result<Void> tryAllUrisPost(String domain, Message msg, Long sid, String srcDomain) {
        long deadline = System.currentTimeMillis() + REMOTE_COMM_DEADLINE;
        Result<Void> res = error(ErrorCode.TIMEOUT);
        while (System.currentTimeMillis() < deadline) {
            var uris = Discovery.getInstance().knownUrisOf("Messages@" + domain, 1);
            for (var uri : uris) {
                res = new RestAdminMessagesRepClient(uri.toString())
                          .tryOncePostWithSid(msg, sid, srcDomain);
                if (res.isOK()) return res;
            }
            sd2526.trab.impl.utils.Sleep.ms(500);
        }
        return res;
    }

    private Result<Void> tryAllUrisDelete(String domain, String mid, Long sid, String srcDomain) {
        long deadline = System.currentTimeMillis() + REMOTE_COMM_DEADLINE;
        Result<Void> res = error(ErrorCode.TIMEOUT);
        while (System.currentTimeMillis() < deadline) {
            var uris = Discovery.getInstance().knownUrisOf("Messages@" + domain, 1);
            for (var uri : uris) {
                res = new RestAdminMessagesRepClient(uri.toString())
                          .tryOnceDeleteWithSid(mid, sid, srcDomain);
                if (res.isOK()) return res;
            }
            sd2526.trab.impl.utils.Sleep.ms(500);
        }
        return res;
    }
}
