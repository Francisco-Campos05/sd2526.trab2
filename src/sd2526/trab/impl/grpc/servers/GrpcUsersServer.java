package sd2526.trab.impl.grpc.servers;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import sd2526.trab.api.java.Users;
import sd2526.trab.impl.db.Hibernate;
import sd2526.trab.impl.discovery.Discovery;

public class GrpcUsersServer extends AbstractGrpcServer {
public static final int PORT = 13456;

	private static Logger Log = Logger.getLogger(GrpcUsersServer.class.getName());

	public GrpcUsersServer() {
		super( Log, Users.SERVICE_NAME, PORT);
	}

	@Override
	protected List<GrpcController> controllers(String uri) {
		return List.of( new GrpcUsersController(), new GrpcAdminUsersController() );
	}

	/**
	 * Override start() so we announce AFTER server.start() — not before.
	 * The users server has an almost-zero construction time (JavaUsers constructor
	 * is empty), so without this override the base class would announce and then
	 * immediately start, leaving a tiny window where the tester can receive the
	 * URI and attempt a connection before the server is actually listening.
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
			// arrives. The server announces itself only after server.start(), by which
			// time this thread has had several seconds to initialize the SessionFactory.
			Thread warmup = new Thread(Hibernate::getInstance);
			warmup.setDaemon(true);
			warmup.start();

			new GrpcUsersServer().start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


}
