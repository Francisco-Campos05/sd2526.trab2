package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.api.java.Messages;
import sd2526.trab.impl.java.servers.JavaMessages;

public class RestMessagesServer extends AbstractRestServer {
	public static final int PORT = 4567;

	private static Logger Log = Logger.getLogger(RestMessagesServer.class.getName());

	RestMessagesServer() {
		super(Log, Messages.SERVICE_NAME, PORT);
	}

	@Override
	void registerResources(ResourceConfig config) {
		config.register(RestMessagesResource.class);
	}

	public static void main(String[] args) {
		JavaMessages.getInstance(); // Eagerly initialize DB before announcing service
		new RestMessagesServer().start();
	}
}