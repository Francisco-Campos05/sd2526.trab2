package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.api.java.Messages;
import sd2526.trab.impl.db.Hibernate;
import sd2526.trab.impl.java.servers.JavaMessages;

public class RestMessagesServer extends AbstractRestServer {
	public static final int PORT = 4567;

	private static Logger Log = Logger.getLogger(RestMessagesServer.class.getName());

	RestMessagesServer(int port) {
		super(Log, Messages.SERVICE_NAME, port);
	}

	@Override
	void registerResources(ResourceConfig config) {
		config.register(RestMessagesResource.class);
	}

	public static void main(String[] args) {
		int port = PORT;
		for (int i = 0; i < args.length - 1; i++) {
			if (args[i].equalsIgnoreCase("-port")) {
				port = Integer.parseInt(args[i + 1]);
				break;
			}
		}
		JavaMessages.getInstance();
		Hibernate.getInstance(); // Eagerly initialize DB schema before announcing service
		new RestMessagesServer(port).start();
	}
}