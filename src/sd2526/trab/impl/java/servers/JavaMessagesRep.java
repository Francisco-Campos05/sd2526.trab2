package sd2526.trab.impl.java.servers;

import static sd2526.trab.api.java.Result.error;
import static sd2526.trab.api.java.Result.ok;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import sd2526.trab.api.Message;
import sd2526.trab.api.User;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Result.ErrorCode;
import sd2526.trab.impl.db.DB;
import sd2526.trab.impl.discovery.Discovery;
import sd2526.trab.impl.java.clients.Clients;
import sd2526.trab.impl.rest.clients.RestAdminMessagesRepClient;

public class JavaMessagesRep extends JavaMessages {

    private static final Logger Log = Logger.getLogger(JavaMessagesRep.class.getName());

    private final ReplicationManager repManager;

    public JavaMessagesRep(ReplicationManager repManager) {
        super();
        this.repManager = repManager;
    }

    public ReplicationManager getRepManager() { return repManager; }

    // ---- Override write operations to apply via replication ----

    @Override
    public Result<String> postMessage(String pwd, Message msg) {
        Log.info(() -> "postMessage (rep): msg=%s".formatted(msg));

        var userResult = getUser(msg.getSender(), pwd);
        if (!userResult.isOK()) return error(userResult.error());

        return doAsyncPostRep(userResult.value(), msg);
    }

    @Override
    public Result<Void> deleteMessage(String name, String mid, String pwd) {
        Log.info(() -> "deleteMessage (rep): name=%s mid=%s".formatted(name, mid));

        var userResult = getUser(name, pwd);
        if (!userResult.isOK()) return error(userResult.error());

        var msgResult = getCachedMessage(mid);
        if (!msgResult.isOK()) return error(ErrorCode.FORBIDDEN);
        var msg = msgResult.value();

        if (!name.equals(getName(msg.senderAddress()))) return error(ErrorCode.FORBIDDEN);

        return applyAndReplicate(ReplicatedOperation.deleteMessage(
                repManager.nextSeqNum(), mid, new HashSet<>(msg.getDestination())));
    }

    @Override
    public Result<Void> remotePostMessage(Message msg) {
        return remotePostWithSid(msg, null, null);
    }

    public Result<Void> remotePostWithSid(Message msg, Long sid, String sourceDomain) {
        if (sid != null && sourceDomain != null && !repManager.checkAndUpdateSid(sourceDomain, sid)) {
            Log.info("Duplicate remote post from " + sourceDomain + " sid=" + sid);
            return ok();
        }
        var localAddresses = localAddressesOf(msg);
        var knownLocal = computeKnownLocal(localAddresses);
        return applyAndReplicate(ReplicatedOperation.remotePost(
                repManager.nextSeqNum(), sid, sourceDomain, msg, knownLocal));
    }

    @Override
    public Result<Void> remoteDeleteMessage(String mid) {
        return remoteDeleteWithSid(mid, null, null);
    }

    public Result<Void> remoteDeleteWithSid(String mid, Long sid, String sourceDomain) {
        if (sid != null && sourceDomain != null && !repManager.checkAndUpdateSid(sourceDomain, sid)) {
            Log.info("Duplicate remote delete from " + sourceDomain + " sid=" + sid);
            return ok();
        }
        return applyAndReplicate(ReplicatedOperation.remoteDelete(
                repManager.nextSeqNum(), sid, sourceDomain, mid));
    }

    @Override
    public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
        Log.info(() -> "removeInboxMessage (rep): name=%s mid=%s".formatted(name, mid));
        var userResult = getUser(name, pwd);
        if (!userResult.isOK()) return error(userResult.error());
        return applyAndReplicate(ReplicatedOperation.removeInboxEntry(
                repManager.nextSeqNum(), mid, name));
    }

    @Override
    public Result<Void> remoteDeleteUserInbox(String name) {
        Log.info(() -> "remoteDeleteUserInbox (rep): name=%s".formatted(name));
        return applyAndReplicate(ReplicatedOperation.deleteInbox(repManager.nextSeqNum(), name));
    }

    // ---- Core replication dispatch ----

    public Result<Void> applyAndReplicate(ReplicatedOperation op) {
        replicateAsync(op);
        var result = executeLocally(op);
        repManager.updateVersion(op.getSeqNum());
        return result;
    }

    private void replicateAsync(ReplicatedOperation op) {
        for (var uri : repManager.getSecondaryUris()) {
            jobs.submit("rep:" + uri, () -> {
                try {
                    new RestAdminMessagesRepClient(uri).applyReplicatedOp(op);
                } catch (Exception e) {
                    Log.warning("Replication to " + uri + " failed: " + e.getMessage());
                }
            });
        }
    }

    public Result<Void> executeLocally(ReplicatedOperation op) {
        return switch (op.getType()) {
            case POST_MESSAGE       -> execPost(op);
            case DELETE_MESSAGE     -> execDelete(op);
            case REMOTE_POST        -> execRemotePost(op);
            case REMOTE_DELETE      -> execRemoteDelete(op);
            case DELETE_INBOX       -> execDeleteInbox(op);
            case REMOVE_INBOX_ENTRY -> execRemoveInboxEntry(op);
        };
    }

    private Result<Void> execPost(ReplicatedOperation op) {
        var msg = op.getMessage();
        messagesCache.put(msg.originId(), new Message(msg));
        messagesCache.put(msg.getId(), msg);

        if (!op.getLocalRecipients().isEmpty())
            persistToInbox(op.getLocalRecipients(), msg);

        if (!op.getRemoteDestinations().isEmpty()) {
            long mySid = op.getSeqNum();
            var targets = op.getRemoteDestinations().stream().collect(
                    Collectors.groupingBy(super::getDomain, Collectors.toSet()));
            for (var e : targets.entrySet()) {
                var domain = e.getKey();
                var domainAddrs = e.getValue();
                jobs.submit(domain, () -> {
                    var res = super.reTry(() -> repClientFor(domain)
                            .remotePostMessageWithSid(msg, mySid, THIS_DOMAIN), REMOTE_COMM_DEADLINE);
                    if (res.error() == ErrorCode.TIMEOUT)
                        domainAddrs.forEach(a ->
                            persistToInbox(List.of(msg.senderAddress()), msg.cloneWithTimeout(a)));
                });
            }
        }
        return ok();
    }

    private Result<Void> execDelete(ReplicatedOperation op) {
        long mySid = op.getSeqNum();
        op.getDestinations().stream().map(r -> r.split("@")[1]).collect(Collectors.toSet())
                .forEach(domain -> {
                    if (domain.equals(THIS_DOMAIN))
                        deleteFromInbox(op.getMessageId());
                    else {
                        String mid = op.getMessageId();
                        jobs.submit(domain, () -> super.reTry(() ->
                            repClientFor(domain).remoteDeleteMessageWithSid(mid, mySid, THIS_DOMAIN),
                            REMOTE_COMM_DEADLINE));
                    }
                });
        return ok();
    }

    private Result<Void> execRemotePost(ReplicatedOperation op) {
        var msg = op.getMessage();
        var known = op.getLocalRecipients() != null ? op.getLocalRecipients() : List.<String>of();
        if (!known.isEmpty()) persistToInbox(known, msg);

        var allLocal = localAddressesOf(msg);
        var unknown = allLocal.stream().filter(a -> !known.contains(a)).toList();
        unknown.forEach(addr -> {
            var errMsg = msg.cloneWithUserNotFound(addr);
            var senderDomain = getDomain(msg.senderAddress());
            if (isLocalDomain(senderDomain))
                persistToInbox(List.of(msg.senderAddress()), errMsg);
            else
                doAsyncRemotePost(senderDomain, errMsg);
        });
        return ok();
    }

    private Result<Void> execRemoteDelete(ReplicatedOperation op) {
        return deleteFromInbox(op.getMessageId());
    }

    private Result<Void> execRemoveInboxEntry(ReplicatedOperation op) {
        var entry = new InboxEntry(op.getMessageId(), op.getUserName());
        return DB.deleteOne(entry).mapToVoid()
                .then(() -> gcDeletedMessageCache.put(op.getMessageId(), op.getMessageId()));
    }

    private Result<Void> execDeleteInbox(ReplicatedOperation op) {
        var sql = "SELECT * FROM InboxEntry e WHERE e.recipient = '%s'".formatted(op.getUserName());
        return DB.transaction(h -> h.select(sql, InboxEntry.class).thenWith(entries -> {
            h.deleteMany(entries);
            entries.forEach(e -> gcDeletedMessageCache.put(e.mid, e.mid));
            return ok();
        }));
    }

    // ---- Post from client (primary only) ----

    public Result<String> doAsyncPostRep(User sender, Message msg) {
        return getCachedMessage(msg.originId()).mapValue(Message::getId).orElse(() -> {
            msg.setId("%s+%04d".formatted(THIS_DOMAIN, counter.incrementAndGet()));
            messagesCache.put(msg.originId(), new Message(msg));
            msg.setSender("%s <%s@%s>".formatted(
                    sender.getDisplayName(), sender.getName(), sender.getDomain()));
            messagesCache.put(msg.getId(), msg);

            var local = localAddressesOf(msg);
            var remote = msg.getDestination().stream()
                    .filter(a -> !isLocalAddress(a)).collect(Collectors.toSet());
            var knownLocal = computeKnownLocal(local);

            return applyAndReplicate(ReplicatedOperation.postMessage(
                    repManager.nextSeqNum(), new Message(msg), knownLocal, remote))
                    .mapValue(__ -> msg.getId())
                    .orElse(() -> ok(msg.getId()));
        });
    }

    // ---- Helpers ----

    private List<String> localAddressesOf(Message msg) {
        return msg.getDestination().stream().filter(super::isLocalAddress).toList();
    }

    private List<String> computeKnownLocal(Collection<String> addresses) {
        if (addresses.isEmpty()) return List.of();
        var unknownResult = checkUsers(addresses);
        if (!unknownResult.isOK()) return List.of();
        var unknown = unknownResult.value();
        return addresses.stream().filter(a -> !unknown.contains(a)).collect(Collectors.toList());
    }

    private void persistToInbox(Collection<String> recipients, Message msg) {
        DB.transaction(h -> {
            h.persistOne(msg);
            for (var r : recipients)
                h.persistOne(new InboxEntry(msg.getId(), getName(r)));
            return ok();
        });
    }

    private Result<Void> deleteFromInbox(String mid) {
        var sql = "SELECT * FROM InboxEntry e WHERE e.mid = '%s'".formatted(mid);
        // Only delete InboxEntry rows — see JavaMessages.deleteFromLocalInbox for reasoning.
        return DB.transaction(h ->
            h.select(sql, InboxEntry.class).thenWith(h::deleteMany)
        );
    }

    private RestAdminMessagesRepClient repClientFor(String domain) {
        var uris = Discovery.getInstance().knownUrisOf("Messages@" + domain, 1);
        return new RestAdminMessagesRepClient(uris[0].toString());
    }
}
