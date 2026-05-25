package sd2526.trab.impl.rest.clients;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import sd2526.trab.api.Message;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.api.rest.RestAdminMessages;
import sd2526.trab.impl.api.rest.RestAdminMessagesRep;
import sd2526.trab.impl.java.servers.ReplicatedOperation;

public class RestAdminMessagesRepClient extends RestClient implements AdminMessages {

    public RestAdminMessagesRepClient(String serverURI) {
        super(serverURI, RestMessages.PATH);
    }

    // ---- AdminMessages (for non-rep domains: plain forwarding) ----

    @Override
    public Result<Void> remotePostMessage(Message m) {
        return remotePostMessageWithSid(m, null, null);
    }

    @Override
    public Result<Void> remoteDeleteMessage(String mid) {
        return remoteDeleteMessageWithSid(mid, null, null);
    }

    @Override
    public Result<Void> remoteDeleteUserInbox(String name) {
        return super.reTry(() -> super.toJavaResult(
                target.path(RestAdminMessages.ADMIN)
                      .path(RestAdminMessages.INBOX)
                      .path(name)
                      .request()
                      .delete()));
    }

    private Result<Void> doPost(Message msg, Long sid, String sourceDomain) {
        var req = target.path(RestAdminMessages.ADMIN).request();

        if (sid != null) {
            req = req.header("X-MESSAGES-SID", sid)
                    .header("X-MESSAGES-SOURCE-DOMAIN", sourceDomain);
        }
        return super.toJavaResult(req.post(Entity.entity(msg, MediaType.APPLICATION_JSON)));
    }

    private Result<Void> doDelete(String mid, Long sid, String sourceDomain) {
        var req = target.path(RestAdminMessages.ADMIN).path(mid).request();

        if (sid != null) {
            req = req.header("X-MESSAGES-SID", sid)
                    .header("X-MESSAGES-SOURCE-DOMAIN", sourceDomain);
        }
        return super.toJavaResult(req.delete());
    }

    // ---- SID-aware variants (for replicated domains) ----

    public Result<Void> remotePostMessageWithSid(Message msg, Long sid, String sourceDomain) {
        return super.reTry(() -> doPost(msg, sid, sourceDomain));
    }

    public Result<Void> remoteDeleteMessageWithSid(String mid, Long sid, String sourceDomain) {
        return super.reTry(() -> doDelete(mid, sid, sourceDomain));
    }

    // ---- Single-shot variants (no internal retry — for URI cycling in execPost/execDelete) ----

    public Result<Void> tryOncePostWithSid(Message msg, Long sid, String sourceDomain) {
        try {
            return doPost(msg, sid, sourceDomain);
        } catch (Exception e) {
            return Result.error(Result.ErrorCode.TIMEOUT);
        }
    }

    public Result<Void> tryOnceDeleteWithSid(String mid, Long sid, String sourceDomain) {
        try {
            return doDelete(mid, sid, sourceDomain);
        } catch (Exception e) {
            return Result.error(Result.ErrorCode.TIMEOUT);
        }
    }

    // ---- Replication-specific endpoints ----

    public Result<Void> applyReplicatedOp(ReplicatedOperation op) {
        return super.reTry(() -> super.toJavaResult(
                target.path(RestAdminMessagesRep.REP_PATH)
                      .request()
                      .post(Entity.entity(op, MediaType.APPLICATION_JSON))));
    }

    public Result<Void> registerReplica(String uri) {
        return super.reTry(() -> super.toJavaResult(
                target.path(RestAdminMessagesRep.REG_PATH)
                      .request()
                      .post(Entity.entity(uri, MediaType.TEXT_PLAIN))));
    }
}
