package sd2526.trab.impl.rest.servers;

import java.net.URI;
import java.util.logging.Logger;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.impl.discovery.Discovery;
import sd2526.trab.impl.java.servers.AbstractServer;
import sd2526.trab.impl.utils.IP;
import sd2526.trab.impl.utils.TLSUtils;

public abstract class AbstractRestServer extends AbstractServer {

    private static final String REST_CTX = "/rest";

    private static String scheme() {
        return TLSUtils.keystoreExists(IP.hostname()) ? "https" : "http";
    }

    protected AbstractRestServer(Logger log, String service, int port) {
        super(log, service, "%s://%s:%d%s".formatted(scheme(), IP.hostname(), port, REST_CTX));
    }

    protected void start() {
        var config = new ResourceConfig();
        registerResources(config);

        var bindUri = URI.create(serverURI.replace(IP.hostname(), INETADDR_ANY));

        if (serverURI.startsWith("https://")) {
            var sslCtx = TLSUtils.serverContext(IP.hostname() + ".ks", TLSUtils.DEFAULT_PWD);
            JdkHttpServerFactory.createHttpServer(bindUri, config, sslCtx);
        } else {
            JdkHttpServerFactory.createHttpServer(bindUri, config);
        }

        if (service != null)
            Discovery.getInstance().announce(serviceName(), serverURI);

        Log.info("%s Server ready @ %s\n".formatted(service, serverURI));
    }

    abstract void registerResources(ResourceConfig config);
}
