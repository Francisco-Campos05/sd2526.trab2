package sd2526.trab.impl.rest.servers;

import java.net.URI;
import java.util.List;

import jakarta.inject.Singleton;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import jakarta.ws.rs.ext.Provider;
import sd2526.trab.api.Message;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.impl.api.rest.RestAdminMessages;
import sd2526.trab.impl.api.rest.RestAdminMessagesRep;
import sd2526.trab.impl.java.servers.JavaMessagesRep;
import sd2526.trab.impl.java.servers.ReplicatedOperation;
import sd2526.trab.impl.java.servers.ReplicationManager;

@Singleton
@Provider
public class RestMessagesRepResource extends RestResource
        implements RestMessages, RestAdminMessages, RestAdminMessagesRep {

    @Context HttpHeaders httpHeaders;
    @Context UriInfo uriInfo;

    private final JavaMessagesRep impl;
    private final ReplicationManager repManager;

    public RestMessagesRepResource(JavaMessagesRep impl, ReplicationManager repManager) {
        this.impl = impl;
        this.repManager = repManager;
    }

    // ---- Version handling ----

    private void checkVersionOrRedirect() {
        if (repManager.isPrimary()) return;
        var clientVersion = VersionHeaderHandler.version.get();
        if (clientVersion != null && repManager.getCurrentVersion() < clientVersion)
            redirectToPrimary();
    }

    private void redirectToPrimary() {
        var pUri = repManager.getPrimaryUri();
        if (pUri == null) throw new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE);
        var req = uriInfo.getRequestUri();
        try {
            var redirect = new URI(URI.create(pUri).getScheme(),
                    URI.create(pUri).getAuthority(),
                    req.getPath(), req.getQuery(), null);
            throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
        } catch (java.net.URISyntaxException e) {
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    private void requirePrimary() {
        if (!repManager.isPrimary()) redirectToPrimary();
    }

    private void setVersionHeader() {
        VersionHeaderHandler.version.set(repManager.getCurrentVersion());
    }

    // ---- Client-facing Messages endpoints ----

    @Override
    public String postMessage(String pwd, Message msg) {
        requirePrimary();
        var result = super.resultOrThrow(impl.postMessage(pwd, msg));
        setVersionHeader();
        return result;
    }

    @Override
    public Message getMessage(String name, String mid, String pwd) {
        checkVersionOrRedirect();
        var result = super.resultOrThrow(impl.getInboxMessage(name, mid, pwd));
        setVersionHeader();
        return result;
    }

    @Override
    public List<String> getMessages(String name, String pwd, String query) {
        checkVersionOrRedirect();
        var result = (query != null && !query.isEmpty())
                ? super.resultOrThrow(impl.searchInbox(name, pwd, query))
                : super.resultOrThrow(impl.getAllInboxMessages(name, pwd));
        setVersionHeader();
        return result;
    }

    @Override
    public void removeFromUserInbox(String name, String mid, String pwd) {
        requirePrimary();
        super.resultOrThrow(impl.removeInboxMessage(name, mid, pwd));
        setVersionHeader();
    }

    @Override
    public void deleteMessage(String name, String mid, String pwd) {
        requirePrimary();
        super.resultOrThrow(impl.deleteMessage(name, mid, pwd));
        setVersionHeader();
    }

    // ---- Admin endpoints (server-to-server) ----

    @Override
    public void remotePostMessage(Message m) {
        requirePrimary();
        Long sid = parseLong(httpHeaders.getHeaderString("X-MESSAGES-SID"));
        String srcDomain = httpHeaders.getHeaderString("X-MESSAGES-SOURCE-DOMAIN");
        if (sid != null && srcDomain != null)
            super.resultOrThrow(impl.remotePostWithSid(m, sid, srcDomain));
        else
            super.resultOrThrow(impl.remotePostMessage(m));
        setVersionHeader();
    }

    @Override
    public void remoteDeleteMessage(String mid) {
        requirePrimary();
        Long sid = parseLong(httpHeaders.getHeaderString("X-MESSAGES-SID"));
        String srcDomain = httpHeaders.getHeaderString("X-MESSAGES-SOURCE-DOMAIN");
        if (sid != null && srcDomain != null)
            super.resultOrThrow(impl.remoteDeleteWithSid(mid, sid, srcDomain));
        else
            super.resultOrThrow(impl.remoteDeleteMessage(mid));
        setVersionHeader();
    }

    @Override
    public void remoteDeleteUserInbox(String name) {
        requirePrimary();
        super.resultOrThrow(impl.remoteDeleteUserInbox(name));
        setVersionHeader();
    }

    // ---- Replication endpoints ----

    @Override
    public void applyReplicatedOp(ReplicatedOperation op) {
        super.resultOrThrow(impl.executeLocally(op));
        repManager.updateVersion(op.getSeqNum());
        setVersionHeader();
    }

    @Override
    public void registerReplica(String uri) {
        if (!repManager.isPrimary())
            throw new WebApplicationException(Response.Status.FORBIDDEN);
        repManager.addSecondary(uri);
    }

    // ---- Helper ----

    private static Long parseLong(String s) {
        if (s == null || s.isEmpty()) return null;
        try { return Long.parseLong(s); } catch (NumberFormatException e) { return null; }
    }
}
