package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.api.java.Messages;
import sd2526.trab.impl.discovery.Discovery;
import sd2526.trab.impl.java.servers.JavaMessagesRep;
import sd2526.trab.impl.java.servers.ReplicationManager;
import sd2526.trab.impl.rest.clients.RestAdminMessagesRepClient;
import sd2526.trab.impl.utils.IP;
import sd2526.trab.impl.utils.Sleep;

public class RestMessagesRepServer extends AbstractRestServer {

    public static final int PORT = 5678;

    private static final Logger Log = Logger.getLogger(RestMessagesRepServer.class.getName());

    private final ReplicationManager repManager;
    private final JavaMessagesRep impl;

    RestMessagesRepServer(boolean isPrimary) {
        super(Log, Messages.SERVICE_NAME, PORT);
        this.repManager = new ReplicationManager(isPrimary);
        this.impl = new JavaMessagesRep(repManager);
    }

    @Override
    void registerResources(ResourceConfig config) {
        config.registerInstances(
                new RestMessagesRepResource(impl, repManager),
                new VersionHeaderHandler());
    }

    @Override
    protected void start() {
        super.start();
        if (repManager.isPrimary()) {
            Discovery.getInstance().announce("MessagesPrimary@" + IP.domain(), serverURI);
            Log.info("Started as PRIMARY");
        } else {
            Log.info("Started as SECONDARY – discovering primary...");
            startSecondaryInit();
        }
    }

    private void startSecondaryInit() {
        repManager.discoverPrimary(IP.domain());
        new Thread(() -> {
            while (repManager.getPrimaryUri() == null)
                Sleep.ms(500);

            var myUri = serverURI;
            while (true) {
                try {
                    new RestAdminMessagesRepClient(repManager.getPrimaryUri())
                            .registerReplica(myUri);
                    Log.info("Registered with primary at " + repManager.getPrimaryUri());
                    return;
                } catch (Exception e) {
                    Log.warning("Could not register with primary, retrying: " + e.getMessage());
                    Sleep.ms(1000);
                }
            }
        }, "secondary-register").start();
    }

    public static void main(String[] args) {
        boolean isPrimary = args.length > 0 && args[0].equalsIgnoreCase("primary");
        Log.info("Starting replicated messages server, role=" + (isPrimary ? "primary" : "secondary"));
        new RestMessagesRepServer(isPrimary).start();
    }
}
