package sd2526.trab.impl.rest.servers;

import java.util.List;

import jakarta.inject.Singleton;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.ext.Provider;
import sd2526.trab.api.Message;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.impl.api.rest.RestAdminMessages;
import sd2526.trab.impl.java.servers.JavaMessagesRep;
import sd2526.trab.impl.java.servers.KafkaReplicationManager;

@Singleton
@Provider
public class RestMessagesRepResource extends RestResource
        implements RestMessages, RestAdminMessages {

    @Context HttpHeaders httpHeaders;

    private final JavaMessagesRep impl;
    private final KafkaReplicationManager kafka;

    public RestMessagesRepResource(JavaMessagesRep impl, KafkaReplicationManager kafka) {
        this.impl  = impl;
        this.kafka = kafka;
    }

    // ---- Version helpers ----

    private void waitForClientVersion() {
        var v = VersionHeaderHandler.version.get();
        if (v != null && kafka.getCurrentOffset() < v)
            kafka.waitForVersion(v);
    }

    private void setVersionHeader() {
        VersionHeaderHandler.version.set(kafka.getCurrentOffset());
    }

    // ---- Client-facing Messages endpoints (any replica handles reads and writes) ----

    @Override
    public String postMessage(String pwd, Message msg) {
        var result = super.resultOrThrow(impl.postMessage(pwd, msg));
        setVersionHeader();
        return result;
    }

    @Override
    public Message getMessage(String name, String mid, String pwd) {
        waitForClientVersion();
        var result = super.resultOrThrow(impl.getInboxMessage(name, mid, pwd));
        setVersionHeader();
        return result;
    }

    @Override
    public List<String> getMessages(String name, String pwd, String query) {
        waitForClientVersion();
        var result = (query != null && !query.isEmpty())
                ? super.resultOrThrow(impl.searchInbox(name, pwd, query))
                : super.resultOrThrow(impl.getAllInboxMessages(name, pwd));
        setVersionHeader();
        return result;
    }

    @Override
    public void removeFromUserInbox(String name, String mid, String pwd) {
        super.resultOrThrow(impl.removeInboxMessage(name, mid, pwd));
        setVersionHeader();
    }

    @Override
    public void deleteMessage(String name, String mid, String pwd) {
        waitForClientVersion(); // message must be in cache before we can check sender + delete
        super.resultOrThrow(impl.deleteMessage(name, mid, pwd));
        setVersionHeader();
    }

    // ---- Admin endpoints (server-to-server cross-domain) ----

    @Override
    public void remotePostMessage(Message m) {
        var sid    = parseLong(httpHeaders.getHeaderString("X-MESSAGES-SID"));
        var srcDom = httpHeaders.getHeaderString("X-MESSAGES-SOURCE-DOMAIN");
        if (sid != null && srcDom != null)
            super.resultOrThrow(impl.remotePostWithSid(m, sid, srcDom));
        else
            super.resultOrThrow(impl.remotePostMessage(m));
        setVersionHeader();
    }

    @Override
    public void remoteDeleteMessage(String mid) {
        var sid    = parseLong(httpHeaders.getHeaderString("X-MESSAGES-SID"));
        var srcDom = httpHeaders.getHeaderString("X-MESSAGES-SOURCE-DOMAIN");
        if (sid != null && srcDom != null)
            super.resultOrThrow(impl.remoteDeleteWithSid(mid, sid, srcDom));
        else
            super.resultOrThrow(impl.remoteDeleteMessage(mid));
        setVersionHeader();
    }

    @Override
    public void remoteDeleteUserInbox(String name) {
        super.resultOrThrow(impl.remoteDeleteUserInbox(name));
        setVersionHeader();
    }

    // ---- Helper ----

    private static Long parseLong(String s) {
        if (s == null || s.isEmpty()) return null;
        try { return Long.parseLong(s); } catch (NumberFormatException e) { return null; }
    }
}
