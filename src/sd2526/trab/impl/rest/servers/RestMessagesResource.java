package sd2526.trab.impl.rest.servers;

import jakarta.inject.Singleton;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;

import sd2526.trab.api.Message;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.rest.RestMessages;

import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.api.rest.RestAdminMessages;
import sd2526.trab.impl.java.clients.Clients;
import sd2526.trab.impl.java.servers.JavaMessages;

import java.util.List;

@Singleton
public class RestMessagesResource extends RestResource implements RestMessages, RestAdminMessages {

    final boolean isGateway;

    /**
     * Non-null when an external implementation (e.g. Zoho-backed) is injected.
     */
    static Messages externalImpl = null;

    /**
     * Called by RestMessagesExternalServer before the server starts.
     */
    public static synchronized void setExternalImpl(Messages impl) {
        externalImpl = impl;
    }

    Messages impl;

    synchronized Messages impl() {
        if (externalImpl != null) return externalImpl;
        if (impl == null)
            impl = isGateway ? Clients.MessagesClient.get() : JavaMessages.getInstance();
        return impl;
    }

    public RestMessagesResource() {
        this.isGateway = false;
    }

    RestMessagesResource(boolean gw) {
        this.isGateway = gw;
    }

    @Override
    public String postMessage(String pwd, Message msg) {
        return super.resultOrThrow(impl().postMessage(pwd, msg));
    }

    @Override
    public Message getMessage(String name, String mid, String pwd) {
        return super.resultOrThrow(impl().getInboxMessage(name, mid, pwd));
    }

    @Override
    public List<String> getMessages(String name, String pwd, String query) {
        if (query != null && !query.isEmpty())
            return super.resultOrThrow(impl().searchInbox(name, pwd, query));
        else
            return super.resultOrThrow(impl().getAllInboxMessages(name, pwd));
    }

    @Override
    public void removeFromUserInbox(String name, String mid, String pwd) {
        super.resultOrThrow(impl().removeInboxMessage(name, mid, pwd));

    }

    @Override
    public void deleteMessage(String name, String mid, String pwd) {
        super.resultOrThrow(impl().deleteMessage(name, mid, pwd));
    }

    private AdminMessages adminImpl() {
        if (impl() instanceof AdminMessages)
            return (AdminMessages) impl();

        throw new WebApplicationException(Response.Status.NOT_IMPLEMENTED);
    }

    @Override
    public void remotePostMessage(Message msg) {
        super.resultOrThrow(adminImpl().remotePostMessage(msg));
    }

    @Override
    public void remoteDeleteMessage(String mid) {
        super.resultOrThrow(adminImpl().remoteDeleteMessage(mid));
    }

    @Override
    public void remoteDeleteUserInbox(String name) {
        super.resultOrThrow(adminImpl().remoteDeleteUserInbox(name));
    }
}
