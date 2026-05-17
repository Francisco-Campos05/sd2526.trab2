package sd2526.trab.impl.grpc.servers;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import sd2526.trab.api.java.Messages;
import sd2526.trab.impl.db.Hibernate;
import sd2526.trab.impl.discovery.Discovery;

public class GrpcMessagesServer extends AbstractGrpcServer {
public static final int PORT = 14567;

	private static Logger Log = Logger.getLogger(GrpcMessagesServer.class.getName());

	public GrpcMessagesServer() {
		super( Log, Messages.SERVICE_NAME, PORT);
	}

	@Override
	protected List<GrpcController> controllers(String uri) {
		return List.of( new GrpcMessagesController(), new GrpcAdminMessagesController() );
	}

	/**
	 * Override start() so we announce AFTER server.start() — not before.
	 * The messages server constructor no longer blocks on Hibernate, so it
	 * completes quickly; without this override the base class would announce
	 * and then immediately start, leaving a window where the tester can receive
	 * the URI and attempt a connection before the server is actually listening.
	 */
	@Override
	protected void start() throws IOException {
		server.start();
		Discovery.getInstance().announce(serviceName(), super.serverURI);
		Log.info(String.format("%s gRPC Server ready @ %s\n", service, serverURI));
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			System.err.println("*** shutting down gRPC server since JVM is shutting down");
			server.shutdownNow();
			System.err.println("*** server shut down");
		}));
	}

	public static void main(String[] args) {
		try {
			// Pre-warm Hibernate in background so it is ready before the first request
			// arrives. The JavaMessages constructor no longer blocks on Hibernate, so
			// the server can start and announce quickly while Hibernate initialises in
			// parallel.
			Thread warmup = new Thread(Hibernate::getInstance);
			warmup.setDaemon(true);
			warmup.start();

			new GrpcMessagesServer().start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
