package sd2526.trab.impl.rest.servers;

import java.util.List;

import jakarta.inject.Singleton;
import sd2526.trab.api.Message;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.api.rest.RestAdminMessages;
import sd2526.trab.impl.java.clients.Clients;
import sd2526.trab.impl.java.servers.JavaMessages;

@Singleton
public class RestMessagesResource extends RestResource implements RestMessages, RestAdminMessages {

	static boolean isGateway = false;

	/** Non-null when an external implementation (e.g. Zoho-backed) is injected. */
	static Messages externalImpl = null;

	/** Called by RestMessagesExternalServer before the server starts. */
	public static synchronized void setExternalImpl(Messages impl) {
		externalImpl = impl;
	}

	Messages impl;

	synchronized Messages impl() {
		if (externalImpl != null) return externalImpl;
		if( impl == null )
			impl = isGateway ? Clients.MessagesClient.get() : JavaMessages.getInstance();
		return impl;
	}

	public RestMessagesResource() {}

	RestMessagesResource(boolean gw) {
		isGateway = gw;
	}
	
	@Override
	public String postMessage(String pwd, Message msg) {
		return super.resultOrThrow( impl().postMessage(pwd, msg));
	}
	
	@Override
	public Message getMessage(String name, String mid, String pwd) {
		return super.resultOrThrow( impl().getInboxMessage(name, mid, pwd));
	}
	
	@Override
	public List<String> getMessages(String name, String pwd, String query) {
		if( query != null && ! query.isEmpty() )
			return super.resultOrThrow( impl().searchInbox(name, pwd, query));
		else
			return super.resultOrThrow(impl().getAllInboxMessages(name, pwd));		
	}
	
	@Override
	public void removeFromUserInbox(String name, String mid, String pwd) {
		super.resultOrThrow( impl().removeInboxMessage(name, mid, pwd) );
		
	}
	
	@Override
	public void deleteMessage(String name, String mid, String pwd) {
		super.resultOrThrow( impl().deleteMessage(name, mid, pwd));
	}

	@Override
	public void remotePostMessage(Message m) {
		super.resultOrThrow( ((AdminMessages)impl()).remotePostMessage(m));
	}

	@Override
	public void remoteDeleteMessage(String mid) {
		super.resultOrThrow( ((AdminMessages)impl()).remoteDeleteMessage(mid));
	}

	@Override
	public void remoteDeleteUserInbox(String name) {
		super.resultOrThrow( ((AdminMessages)impl()).remoteDeleteUserInbox(name));
	}
}
