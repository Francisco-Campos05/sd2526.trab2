package sd2526.trab.impl.grpc.servers;


import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import sd2526.trab.impl.discovery.Discovery;
import sd2526.trab.impl.java.servers.AbstractServer;
import sd2526.trab.impl.utils.IP;
import sd2526.trab.impl.utils.TLSUtils;


public abstract class AbstractGrpcServer extends AbstractServer {
	private static final String SERVER_BASE_URI = "%s://%s:%s%s";

	private static final String GRPC_CTX = "/grpc";

	protected final Server server;

	protected AbstractGrpcServer(Logger log, String service, int port) {
		super(log, service, buildUri(port));

		var hostname = IP.hostname();
		ServerBuilder<?> builder;
		if (TLSUtils.keystoreExists(hostname)) {
			try {
				var kmf = TLSUtils.keyManagerFactory(hostname + ".ks", TLSUtils.DEFAULT_PWD);
				var sslCtx = GrpcSslContexts.configure(SslContextBuilder.forServer(kmf)).build();
				builder = NettyServerBuilder.forPort(port).sslContext(sslCtx);
			} catch (Exception e) {
				throw new RuntimeException("Failed to configure gRPC TLS for " + hostname, e);
			}
		} else {
			builder = ServerBuilder.forPort(port);
		}

		for( var s : controllers( super.serverURI ) )
			builder.addService( s );

		this.server = builder.build();
	}

	private static String buildUri(int port) {
		var hostname = IP.hostname();
		return String.format(SERVER_BASE_URI, "grpc", hostname, port, GRPC_CTX);
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
