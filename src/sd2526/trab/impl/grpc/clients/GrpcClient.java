package sd2526.trab.impl.grpc.clients;

import static sd2526.trab.api.java.Result.error;
import static sd2526.trab.api.java.Result.ok;
import static sd2526.trab.api.java.Result.ErrorCode.INTERNAL_ERROR;

import java.net.URI;
import java.util.function.Supplier;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Result.ErrorCode;
import sd2526.trab.impl.utils.IP;
import sd2526.trab.impl.utils.TLSUtils;

public class GrpcClient {

	final protected URI serverURI;
	final protected Channel channel;

	protected GrpcClient(String serverUrl) {
		this.serverURI = URI.create(serverUrl);
		// Use TLS only when this container is itself a TLS server (has its own keystore).
		// This ensures that plaintext-server containers also use plaintext for outgoing gRPC calls,
		// preventing a TLS client → plaintext server mismatch in mixed-TLS test environments.
		var tmf = TLSUtils.keystoreExists(IP.hostname()) ? TLSUtils.getTrustManagerFactory() : null;
		if (tmf != null) {
			try {
				var sslCtx = GrpcSslContexts.forClient().trustManager(tmf).build();
				this.channel = NettyChannelBuilder
						.forAddress(serverURI.getHost(), serverURI.getPort())
						.sslContext(sslCtx)
						.enableRetry()
						.build();
			} catch (Exception e) {
				throw new RuntimeException("Failed to create gRPC TLS channel to " + serverUrl, e);
			}
		} else {
			this.channel = ManagedChannelBuilder.forAddress(serverURI.getHost(), serverURI.getPort())
					.usePlaintext().enableRetry().build();
		}
	}

	protected <T> Result<T> toJavaResult(Supplier<T> func) {
		try {
			return ok(func.get());
		} catch (StatusRuntimeException sre) {
			//sre.printStackTrace();
			return error(statusToErrorCode(sre.getStatus()));
		} catch (Exception x) {
			x.printStackTrace();
			return Result.error(INTERNAL_ERROR);
		}
	}

	protected Result<Void> toJavaResult(Runnable proc) {
		return toJavaResult( () -> {
			proc.run();
			return null;
		} );
	}

	protected static ErrorCode statusToErrorCode(Status status) {
		return switch (status.getCode()) {
		case OK -> ErrorCode.OK;
		case NOT_FOUND -> ErrorCode.NOT_FOUND;
		case ALREADY_EXISTS -> ErrorCode.CONFLICT;
		case PERMISSION_DENIED -> ErrorCode.FORBIDDEN;
		case INVALID_ARGUMENT -> ErrorCode.BAD_REQUEST;
		case UNIMPLEMENTED -> ErrorCode.NOT_IMPLEMENTED;
		default -> ErrorCode.INTERNAL_ERROR;
		};
	}

	@Override
	public String toString() {
		return serverURI.toString();
	}
}
