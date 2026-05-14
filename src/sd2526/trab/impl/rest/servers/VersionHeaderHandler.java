package sd2526.trab.impl.rest.servers;

import java.io.IOException;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;
import sd2526.trab.api.rest.RestMessages;

@Provider
public class VersionHeaderHandler implements ContainerRequestFilter, ContainerResponseFilter {

    public static final ThreadLocal<Long> version = new ThreadLocal<>();

    @Override
    public void filter(ContainerRequestContext req) throws IOException {
        var val = req.getHeaderString(RestMessages.HEADER_VERSION);
        if (val != null && !val.isEmpty()) {
            try {
                version.set(Long.valueOf(val));
            } catch (NumberFormatException ignored) {}
        } else {
            version.remove();
        }
    }

    @Override
    public void filter(ContainerRequestContext req, ContainerResponseContext res) throws IOException {
        var val = version.get();
        if (val != null)
            res.getHeaders().add(RestMessages.HEADER_VERSION, Long.toString(val));
        version.remove();
    }
}
