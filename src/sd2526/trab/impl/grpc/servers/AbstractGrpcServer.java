package sd2526.trab.impl.grpc.servers;


import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import sd2526.trab.impl.discovery.Discovery;
import sd2526.trab.impl.java.servers.AbstractServer;
import sd2526.trab.impl.utils.IP;


public abstract class AbstractGrpcServer extends AbstractServer {
	private static final String SERVER_BASE_URI = "grpc://%s:%s%s";

	private static final String GRPC_CTX = "/grpc";

	protected final Server server;

	protected AbstractGrpcServer(Logger log, String service, int port) {
		super(log, service, String.format(SERVER_BASE_URI, IP.hostname(), port, GRPC_CTX));
		
		var builder = ServerBuilder.forPort(port);
		for( var s : controllers( super.serverURI ) )
			builder.addService( s );
		
		this.server = builder.build();
	}

	protected abstract List<GrpcController> controllers( String uri );
	
	protected void start() throws IOException {
		
		Discovery.getInstance().announce(serviceName(), super.serverURI);
		
		Log.info(String.format("%s gRPC Server ready @ %s\n", service, serverURI));

		server.start();
		Runtime.getRuntime().addShutdownHook(new Thread( () -> {
			System.err.println("*** shutting down gRPC server since JVM is shutting down");
			server.shutdownNow();
			System.err.println("*** server shut down");
		}));
	}
	
}
