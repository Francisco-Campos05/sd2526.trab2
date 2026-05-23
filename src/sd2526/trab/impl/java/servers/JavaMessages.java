package sd2526.trab.impl.java.servers;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import sd2526.trab.api.Message;
import sd2526.trab.api.User;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Result.ErrorCode;
import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.db.DB;
import sd2526.trab.impl.db.Hibernate;
import sd2526.trab.impl.java.clients.Clients;
import sd2526.trab.impl.utils.IP;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static sd2526.trab.api.java.Result.ErrorCode.*;
import static sd2526.trab.api.java.Result.error;
import static sd2526.trab.api.java.Result.ok;

public class JavaMessages extends JavaBaseService implements Messages, AdminMessages {

    private static final long MESSAGES_CACHE_EXPIRATION = 30000;
    private static final long DIRTY_INBOX_CACHE_EXPIRATION = 10000;

    protected final JobDispatcher jobs;
    protected final AtomicLong counter = new AtomicLong(0L);
    protected static final int REMOTE_COMM_DEADLINE = 90000;
    private static Logger Log = Logger.getLogger(JavaMessages.class.getName());

    /**
     * Cache for messages. Used for two purposes:
     * 1. Keyed by originId: Ensures idempotency of post operations.
     * 2. Keyed by messageId (mid): Allows quick lookup for delete operations.
     */
    protected final Cache<String, Message> messagesCache = CacheBuilder.newBuilder()
            .expireAfterWrite(Duration.ofMillis(MESSAGES_CACHE_EXPIRATION))
            .build();

    protected final Cache<String, String> gcDeletedMessageCache = CacheBuilder.newBuilder()
            .expireAfterWrite(Duration.ofMillis(DIRTY_INBOX_CACHE_EXPIRATION))
            .build();
    // NOTE: removal listener intentionally removed — the background GC sweep
    // created a race condition where a Message with a pending InboxEntry could
    // be incorrectly classified as orphaned and deleted (visible under TLS
    // because TLS latency widens the delivery window). Orphan cleanup is not
    // required for correctness.

    protected JavaMessages() {
        this.jobs = new JobDispatcher();
        Hibernate.getInstance(); // Eagerly initialize Hibernate/DB schema
    }

    @Override
    public Result<String> postMessage(String pwd, Message msg) {
        Log.info(() -> "postMessage : pwd = %s, msg = %s\n".formatted(pwd, msg));

        return getUser(msg.getSender(), pwd)
                .thenWith((user) -> doAsyncPost(user, msg));
    }

    @Override
    public Result<Message> getInboxMessage(String name, String mid, String pwd) {
        Log.info(() -> "getInboxMessage : name = %s, mid = %s, pwd = %s\n".formatted(name, mid, pwd));

        if (badParams(name, mid, pwd))
            return error(BAD_REQUEST);

        return getUser(name, pwd)
                .then(() -> DB.getOne(new InboxEntry(mid, name), InboxEntry.class))
                .then(() -> DB.getOne(mid, Message.class));
    }

    @Override
    public Result<List<String>> getAllInboxMessages(String name, String pwd) {
        Log.info(() -> "getAllInboxMessages : name = %s, pwd = %s\n".formatted(name, pwd));

        var sqlExpr = "SELECT m.mid FROM InboxEntry m WHERE m.recipient = '%s'".formatted(name);
        return getUser(name, pwd)
                .then(() -> DB.select(sqlExpr, String.class));
    }

    @Override
    public Result<List<String>> searchInbox(String name, String pwd, String query) {
        Log.info(() -> "searchInbox : name = %s, pwd = %s, query=%s\n".formatted(name, pwd, query));

        var safeQuery = query.toUpperCase().replace("'", "''").replace("?", "_");
        var sqlExpr = """
                SELECT m.id FROM Message m
                INNER JOIN InboxEntry e
                ON e.mid = m.id
                AND e.recipient = '%s'
                WHERE (upper(m.subject) LIKE '%%%s%%' OR upper(m.contents) LIKE '%%%s%%')
                """.formatted(name, safeQuery, safeQuery);

        return getUser(name, pwd)
                .then(() -> DB.select(sqlExpr, String.class));
    }

    @Override
    public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
        Log.info(() -> "removeInboxMessage : name = %s, mid = %s, pwd = %s\n".formatted(name, mid, pwd));

        return getUser(name, pwd)
                .then(() -> DB.deleteOne(new InboxEntry(mid, name))).mapToVoid()
                .then(() -> gcDeletedMessageCache.put(mid, mid));
    }

    @Override
    public Result<Void> deleteMessage(String name, String mid, String pwd) {
        Log.info(() -> "deleteMessage : name = %s, mid = %s, pwd = %s\n".formatted(name, mid, pwd));


        return getUser(name, pwd)
                .then(() -> getCachedMessage(mid))
                .thenWith(msg -> name.equals(getName(msg.senderAddress())) ? ok(msg) : error(FORBIDDEN))
                .thenWith((msg) -> doAsyncDelete(msg));
    }


    protected Result<User> getUser(String user, String pwd) {
        try {
            var name = user.split("@", 2)[0];
            return Clients.UsersClient.get().getUser(name, pwd);
        } catch (Exception x) {
            x.printStackTrace();
            return Result.error(INTERNAL_ERROR);
        }
    }

    protected Result<Set<String>> checkUsers(Collection<String> addresses) {
        return Clients.AdminUsersClient.get().checkUsers(addresses);
    }

    private void deliverToKnownLocalRecipients(Collection<String> addresses, Message msg) {
        Log.info(() -> "deliverToKnownLocalRecipients : local known addresses = %s, msg = %s\n".formatted(addresses, msg));

        DB.transaction((hibernate) -> {
            hibernate.persistOne(msg);
            for (var address : addresses)
                hibernate.persistOne(new InboxEntry(msg.getId(), getName(address)));

            return ok();
        });

    }

    private void reportUnknownLocalRecipients(Collection<String> addresses, Message msg) {
        Log.info(() -> "reportUnknownLocalRecipients : unknown addresses = %s, msg = %s\n".formatted(addresses, msg));

        var senderDomain = super.getDomain(msg.senderAddress());

        try {
            for (var recipientAddress : addresses) {
                var errorMsg = msg.cloneWithUserNotFound(recipientAddress);
                if (super.isLocalDomain(senderDomain)) {
                    DB.transaction((hibernate) -> {
                        hibernate.persistOne(new InboxEntry(errorMsg.getId(), msg.senderName()));
                        hibernate.persistOne(errorMsg);
                        return ok();
                    });
                } else doAsyncRemotePost(senderDomain, errorMsg);
            }
        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    private Result<Void> postToLocalInboxes(Collection<String> addresses, Message msg) {
        Log.info(() -> "postToLocalInboxes : localRecipients = %s, msg = %s\n".formatted(addresses, msg));

        return checkUsers(addresses)
                .thenWith(unknownAddresses -> {

                    var knownAddresses = new HashSet<>(addresses);
                    knownAddresses.removeAll(unknownAddresses);

                    if (!knownAddresses.isEmpty())
                        deliverToKnownLocalRecipients(knownAddresses, msg);

                    if (!unknownAddresses.isEmpty())
                        reportUnknownLocalRecipients(unknownAddresses, msg);

                    return ok();
                });
    }

    @Override
    public Result<Void> remotePostMessage(Message msg) {
        Log.info(() -> "postRemoteMessage : msg = %s\n".formatted(msg));

        var localAddresses = getLocalRecipientAddresses(msg);
        return postToLocalInboxes(localAddresses, msg);
    }

    private Result<Void> deleteFromLocalInbox(String mid) {
        Log.info(() -> "deleteFromLocalInbox : mid = %s\n".formatted(mid));

        var sql = "SELECT * FROM InboxEntry e WHERE e.mid = '%s'".formatted(mid);

        // Only delete InboxEntry rows — do NOT delete the Message record here.
        // Deleting the Message causes Hibernate to also remove Message_destination rows,
        // which creates serialization conflicts with concurrent Message_destination inserts
        // (for other messages) under HSQLDB. Orphaned Message rows are harmless because
        // all user-facing queries (search, getAll) use INNER JOIN with InboxEntry.
        return DB.transaction(hibernate ->
                hibernate.select(sql, InboxEntry.class)
                        .thenWith((entries) -> hibernate.deleteMany(entries))
        );
    }

    @Override
    public Result<Void> remoteDeleteMessage(String mid) {
        Log.info(() -> "remoteDeleteMessage : mid = %s\n".formatted(mid));

        return deleteFromLocalInbox(mid);
    }

    protected Result<Message> getCachedMessage(String mid) {
        var msg = messagesCache.getIfPresent(mid);
        return msg != null ? ok(msg) : error(FORBIDDEN);
    }

    public final class JobDispatcher {

        private final ConcurrentHashMap<String, ExecutorService> executors = new ConcurrentHashMap<>();

        public void submit(String domain, Runnable job) {
            ExecutorService executor = executors.computeIfAbsent(
                    domain,
                    d -> Executors.newSingleThreadExecutor(r -> {
                        Thread t = new Thread(r);
                        t.setUncaughtExceptionHandler((thr, ex) -> ex.printStackTrace());
                        return t;
                    })
            );
            executor.submit(job);
        }
    }

    public Result<String> doAsyncPost(User sender, Message msg) {

        return getCachedMessage(msg.originId()).mapValue(Message::getId).orElse(() -> {


            msg.setId("%s+%04d".formatted(THIS_DOMAIN, counter.incrementAndGet()));

            messagesCache.put(msg.originId(), new Message(msg)); // For ensuring idempotency...

            msg.setSender("%s <%s@%s>".formatted(sender.getDisplayName(), sender.getName(), sender.getDomain()));

            messagesCache.put(msg.getId(), msg); // For enabling delete of messages...

            var localAddresses = getLocalRecipientAddresses(msg);
            var remoteAddresses = getRemoteRecipientAddresses(msg);

            Log.info(() -> "Local Recipients:" + localAddresses);
            Log.info(() -> "Remote Recipients:" + remoteAddresses);

            if (!localAddresses.isEmpty())
                postToLocalInboxes(localAddresses, msg);

            if (!remoteAddresses.isEmpty()) {

                var remoteTargets = remoteAddresses.stream().collect(
                        Collectors.groupingBy(super::getDomain, Collectors.mapping(address -> address, Collectors.toSet())));

                for (var e : remoteTargets.entrySet()) {
                    var domain = e.getKey();
                    var domainRecipientAddresses = e.getValue();

                    jobs.submit(domain, () -> {
                        var res = super.reTry(() -> Clients.AdminMessagesClient.get(domain).remotePostMessage(msg), REMOTE_COMM_DEADLINE);
                        if (res.error() == ErrorCode.TIMEOUT) {
                            for (var address : domainRecipientAddresses)
                                postToLocalInboxes(Set.of(msg.senderAddress()), msg.cloneWithTimeout(address));
                        }
                    });

                }
            }
            return Result.ok(msg.getId());
        });
    }

    public Result<Void> doAsyncDelete(Message msg) {
        var domains = msg.getDestination().stream().map(r -> r.split("@")[1]).collect(Collectors.toSet());
        for (var domain : domains)
            if (domain.equals(IP.domain()))
                deleteFromLocalInbox(msg.getId());
            else
                jobs.submit(domain, () -> super.reTry(() ->
                        Clients.AdminMessagesClient.get(domain).remoteDeleteMessage(msg.getId()), REMOTE_COMM_DEADLINE));
        return Result.ok();
    }

    public void doAsyncRemotePost(String remoteDomain, Message msg) {
        Log.info(() -> "\nenqueueRemotePost : remoteDomain=%s, msg = %s\n".formatted(remoteDomain, msg));
        jobs.submit(remoteDomain, () ->
            super.reTry(() -> Clients.AdminMessagesClient.get(remoteDomain).remotePostMessage(msg), REMOTE_COMM_DEADLINE));
    }

    @Override
    public Result<Void> remoteDeleteUserInbox(String name) {
        Log.info(() -> "remoteDeleteUserInbox : name = %s\n".formatted(name));

        var sqlExpr = "SELECT * FROM InboxEntry e WHERE e.recipient = '%s'".formatted(name);

        return DB.transaction(hibernate -> hibernate.select(sqlExpr, InboxEntry.class)
                .thenWith((entries) -> {
                    hibernate.deleteMany(entries);
                    for (var e : entries)
                        gcDeletedMessageCache.put(e.mid, e.mid);

                    return ok();
                }));

    }


    private List<String> getLocalRecipientAddresses(Message msg) {
        return msg.getDestination().stream().filter(super::isLocalAddress).toList();
    }

    private Set<String> getRemoteRecipientAddresses(Message msg) {
        return msg.getDestination().stream().filter(Predicate.not(super::isLocalAddress)).collect(Collectors.toSet());
    }


    static JavaMessages instance;

    public static synchronized JavaMessages getInstance() {
        if (instance == null)
            instance = new JavaMessages();
        return instance;
    }
}

