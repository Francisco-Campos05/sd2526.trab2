package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.api.java.Messages;
import sd2526.trab.impl.java.servers.JavaMessagesExternal;

/**
 * REST Messages server backed by Zoho Mail (E1 external service).
 *
 * <p>Command-line arguments:
 * <ol>
 *   <li>Optional first arg: {@code "true"} to clear all stored state on startup,
 *       {@code "false"} (or absent) to reuse saved state.</li>
 *   <li>Optional {@code -port <number>} to override the default port (4568).</li>
 * </ol>
 *
 * <p>This server registers itself under the {@code Messages} service name in
 * Discovery (via {@link AbstractRestServer#start()}), so other domains can
 * deliver messages to this domain's users just like any other Messages server.
 */
public class RestMessagesExternalServer extends AbstractRestServer {

    public static final int PORT = 4568;

    private static final Logger Log = Logger.getLogger(RestMessagesExternalServer.class.getName());

    RestMessagesExternalServer(int port) {
        super(Log, Messages.SERVICE_NAME, port);
    }

    @Override
    void registerResources(ResourceConfig config) {
        config.register(RestMessagesResource.class);
    }

    public static void main(String[] args) {
        // --- Parse clear-state flag (first arg) ---
        boolean clearState = false;
        if (args.length > 0) {
            clearState = Boolean.parseBoolean(args[0]);
        }

        // --- Parse optional -port override ---
        int port = PORT;
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equalsIgnoreCase("-port")) {
                port = Integer.parseInt(args[i + 1]);
                break;
            }
        }

        Log.info("RestMessagesExternalServer starting: port=" + port + ", clearState=" + clearState);

        // Build the Zoho-backed implementation and inject it before the server starts
        JavaMessagesExternal impl = new JavaMessagesExternal(clearState);
        RestMessagesResource.setExternalImpl(impl);

        new RestMessagesExternalServer(port).start();
    }
}
