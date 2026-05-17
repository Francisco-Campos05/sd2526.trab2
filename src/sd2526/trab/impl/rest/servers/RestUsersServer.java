package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.api.java.Users;

public class RestUsersServer extends AbstractRestServer {
	public static final int PORT = 3456;

	private static Logger Log = Logger.getLogger(RestUsersServer.class.getName());

	RestUsersServer() {
		super( Log, Users.SERVICE_NAME , PORT);
	}

	@Override
	void registerResources(ResourceConfig config) {
		config.register(RestUsersResource.class );
	}

	public static void main(String[] args) {
		new RestUsersServer().start();
	}
}