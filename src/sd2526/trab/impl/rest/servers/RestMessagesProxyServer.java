package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.api.java.Messages;
import sd2526.trab.impl.java.servers.JavaMessages;

/**
 * Proxy messages server — listens on port 4568, forwards all requests to a
 * real Messages server discovered via Discovery (gateway mode).
 * Used by the TP2 tester as MESSAGES_REST_PROXY_SERVER_MAINCLASS.
 */
public class RestMessagesProxyServer extends AbstractRestServer {
	public static final int PORT = 4568;

	private static Logger Log = Logger.getLogger(RestMessagesProxyServer.class.getName());

	RestMessagesProxyServer() {
		super(Log, Messages.SERVICE_NAME, PORT);
	}

	@Override
	void registerResources(ResourceConfig config) {
		// Use RestMessagesResource directly (non-gateway, standalone DB).
		// The proxy server acts as an independent messages server on port 4568 —
		// it stores messages locally and serves requests from its own DB.
		// Gateway mode (forwarding to another server) is not used here because:
		//   (a) in tests with a proxy-only domain the proxy IS the sole server, so
		//       forwarding would loop back to itself;
		//   (b) in tests with both a regular server and a proxy, the message is
		//       delivered to whichever server the sender discovers first; the
		//       tester then reads from the same server it delivered to.
		config.register(RestMessagesResource.class);
	}

	public static void main(String[] args) {
		JavaMessages.getInstance(); // Eagerly initialize DB before announcing service
		new RestMessagesProxyServer().start();
	}
}
