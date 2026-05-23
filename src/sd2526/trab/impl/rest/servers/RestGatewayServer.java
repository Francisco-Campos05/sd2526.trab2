package sd2526.trab.impl.rest.servers;

import org.glassfish.jersey.server.ResourceConfig;

import java.util.logging.Logger;

public class RestGatewayServer extends AbstractRestServer {

    public static final int PORT = 6666;

    private static Logger Log = Logger.getLogger(RestGatewayServer.class.getName());

    RestGatewayServer() {
        super(Log, null, PORT);
    }

    @Override
    void registerResources(ResourceConfig config) {
        config.registerInstances(new RestUsersResource(true), new RestMessagesResource(true));
    }

    public static void main(String[] args) {
        new RestGatewayServer().start();
    }
}