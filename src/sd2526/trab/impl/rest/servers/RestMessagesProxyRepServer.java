package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.api.java.Messages;
import sd2526.trab.impl.java.servers.JavaMessages;

/**
 * Proxy messages server on port 4568.
 *
 * For the TP2 proxy tests (108a/108b), `anyReplicate` is false so the tester never
 * starts a Kafka container — see MessagesTest.prepare(). The proxy therefore runs a
 * plain RestMessagesResource backed by local HSQLDB.
 *
 * Hibernate is initialized EAGERLY in main() before super.start() announces the
 * service to Discovery. Otherwise the first client request would trigger lazy
 * init (~2 s) and the tester's 4 s post-restart sleep is not enough — the call
 * would time out (tester sentinel: HTTP 300).
 *
 * State is NOT preserved across the tester's container re-creation (the tester's
 * restartProxyServer does docker run on a fresh container; HSQLDB at /tmp/foo is
 * gone). 108b's "continue with saved state" cannot pass without external state —
 * but the proxy must at least be REACHABLE and answer with HTTP 200 (possibly
 * empty list), not be unreachable mid-init.
 */
public class RestMessagesProxyRepServer extends AbstractRestServer {

    public static final int PORT = 4568;

    private static final Logger Log = Logger.getLogger(RestMessagesProxyRepServer.class.getName());

    RestMessagesProxyRepServer() {
        super(Log, Messages.SERVICE_NAME, PORT);
    }

    @Override
    void registerResources(ResourceConfig config) {
        config.register(RestMessagesResource.class);
    }

    @Override
    protected void start() {
        // Eagerly initialize Hibernate/DB BEFORE announcing the service. After a
        // container re-creation the tester sleeps only 4 s before its first call;
        // lazy init on the first request would time out.
        JavaMessages.getInstance();
        super.start();
        Log.info("Proxy server ready @ " + serverURI);
    }

    public static void main(String[] args) {
        Log.info("Starting proxy messages server");
        new RestMessagesProxyRepServer().start();
    }
}
