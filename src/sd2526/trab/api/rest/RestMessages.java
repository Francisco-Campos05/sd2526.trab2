package sd2526.trab.api.rest;

import java.util.List;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import sd2526.trab.api.Message;

@Path(RestMessages.PATH)
public interface RestMessages {
	
	final String PATH = "/messages";
	final String HEADER_VERSION = "X-MESSAGES-version";
	final String QUERY = "query";
	final String NAME = "name";
	final String PWD = "pwd";
	final String MID = "mid";
	final String MBOX = "/mbox";

	@POST
	@Path("/")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	String postMessage(@QueryParam(PWD) String pwd, Message msg);

	@GET
	@Path(MBOX + "/{" + NAME + "}/{" + MID + "}")
	@Produces(MediaType.APPLICATION_JSON)
	Message getMessage(@PathParam(NAME) String name, @PathParam(MID) String mid, @QueryParam(PWD) String pwd);

	@GET
	@Path(MBOX + "/{" + NAME + "}")
	@Produces(MediaType.APPLICATION_JSON)
	List<String> getMessages(@PathParam(NAME) String name, @QueryParam(PWD) String pwd, @QueryParam(QUERY) @DefaultValue("") String query);

	@DELETE
	@Path(MBOX + "/{" + NAME + "}/{" + MID + "}")
	void removeFromUserInbox(@PathParam(NAME) String name, @PathParam(MID) String mid, @QueryParam(PWD) String pwd);

	@DELETE
	@Path("/{" + NAME + "}/{" + MID + "}")
	void deleteMessage(@PathParam(NAME) String name, @PathParam(MID) String mid, @QueryParam(PWD) String pwd);
}
