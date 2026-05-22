package sd2526.trab.impl.rest.servers;

import java.util.UUID;
import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.api.java.Messages;
import sd2526.trab.impl.java.servers.JavaMessagesRep;
import sd2526.trab.impl.java.servers.KafkaReplicationManager;
import sd2526.trab.impl.utils.IP;

public class RestMessagesRepServer extends AbstractRestServer {

    public static final int PORT = 5678;

    private static final Logger Log = Logger.getLogger(RestMessagesRepServer.class.getName());

    private final KafkaReplicationManager kafka;
    private final JavaMessagesRep impl;

    RestMessagesRepServer() {
        super(Log, Messages.SERVICE_NAME, PORT);
        this.kafka = new KafkaReplicationManager(IP.domain());
        this.impl  = new JavaMessagesRep(kafka, serverURI);
    }

    @Override
    void registerResources(ResourceConfig config) {
        config.registerInstances(
                new RestMessagesRepResource(impl, kafka),
                new VersionHeaderHandler());
    }

    @Override
    protected void start() {
        super.start();
        // Start Kafka consumer AFTER server is announced so we accept requests immediately
        // UUID ensures each container start gets a fresh group (always replays from offset 0)
        String groupId = IP.hostname() + ":" + PORT + ":" + UUID.randomUUID();
        kafka.startConsumer(groupId, impl::executeLocally);
        Log.info("F1 replica started, groupId=" + groupId + ", uri=" + serverURI);
    }

    public static void main(String[] args) {
        Log.info("Starting F1 (Kafka) replicated messages server");
        new RestMessagesRepServer().start();
    }
}
