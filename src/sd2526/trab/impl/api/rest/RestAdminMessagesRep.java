package sd2526.trab.impl.api.rest;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.MediaType;
import sd2526.trab.impl.java.servers.ReplicatedOperation;

public interface RestAdminMessagesRep {

    String REP_PATH  = "/rep";
    String REG_PATH  = "/rep/register";

    @POST
    @Path(REP_PATH)
    @Consumes(MediaType.APPLICATION_JSON)
    void applyReplicatedOp(ReplicatedOperation op);

    @POST
    @Path(REG_PATH)
    @Consumes(MediaType.TEXT_PLAIN)
    void registerReplica(String uri);
}
