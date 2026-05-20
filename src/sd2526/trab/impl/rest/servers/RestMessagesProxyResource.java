package sd2526.trab.impl.rest.servers;

/**
 * JAX-RS resource for the proxy messages server.
 * Extends RestMessagesResource with isGateway=true so all client calls
 * are forwarded to a real Messages server discovered via Discovery,
 * rather than being served from a local DB.
 *
 * A separate class is needed because Jersey requires a no-arg constructor
 * to instantiate resource classes; instance registration (register(Object))
 * only works for providers, not resources.
 */
public class RestMessagesProxyResource extends RestMessagesResource {
    public RestMessagesProxyResource() {
        super(true);
    }
}
