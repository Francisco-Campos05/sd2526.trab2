package sd2526.trab.impl.java.servers;

import static sd2526.trab.api.java.Result.error;
import static sd2526.trab.api.java.Result.ok;
import static sd2526.trab.api.java.Result.ErrorCode.BAD_REQUEST;
import static sd2526.trab.api.java.Result.ErrorCode.CONFLICT;
import static sd2526.trab.api.java.Result.ErrorCode.FORBIDDEN;
import static sd2526.trab.impl.java.clients.Clients.AdminMessagesClient;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import sd2526.trab.api.User;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Result.ErrorCode;
import sd2526.trab.api.java.Users;
import sd2526.trab.impl.api.java.AdminUsers;
import sd2526.trab.impl.db.DB;


public class JavaUsers extends JavaBaseService implements Users, AdminUsers {

	private static Logger Log = Logger.getLogger(JavaUsers.class.getName());

	private JavaUsers() {
	}

	@Override
	public Result<String> postUser(User user) {
		Log.info(() -> "postUser: user=%s\n".formatted(user));

		if( badUserInfo( user ) )
			return error(BAD_REQUEST);

		var userAddress = "%s@%s".formatted(user.getName(), THIS_DOMAIN);

		return DB.getOne( user.getName(), User.class )
				.thenWith( other -> user.matches( other ) ? ok( userAddress ): error( CONFLICT )	)
				.orElse( () -> {
							user.setDomain( THIS_DOMAIN );
							var insertResult = DB.persistOne(user).mapValue( __ -> userAddress );
							if (insertResult.isOK()) return insertResult;
							// Handle concurrent duplicate-insert race (e.g. gateway retry during
							// Hibernate lazy-init): if the user that beat us matches, return 200.
							return DB.getOne(user.getName(), User.class)
									.thenWith(other -> user.matches(other) ? ok(userAddress) : error(CONFLICT));
				});
	}

	@Override
	public Result<User> getUser(String name, String pwd) {
		Log.info( () -> "getUser : name = %s, pwd = %s\n".formatted(name, pwd));

		if (badParams( name, pwd ) )
			return error(BAD_REQUEST);

		return fetchUser(name, pwd);
	}

	@Override
	public Result<User> updateUser(String name, String pwd, User info) {
		Log.info(() -> "updateUser : name = %s, pwd = %s, info: %s\n".formatted(name, pwd, info));
		
		if (badUpdateUserInfo(name, pwd, info))
			return error(BAD_REQUEST);
		
		return fetchUser(name, pwd)
				.thenWith( user -> DB.updateOne(user.updateFrom(info)));
				
	}

	@Override
	public Result<User> deleteUser(String name, String pwd) {
		Log.info(() -> "deleteUser : name = %s, pwd = %s\n".formatted(name, pwd));

		if (badParams(name, pwd ) )
			return error(BAD_REQUEST);

		return fetchUser(name, pwd )
				.thenWith( (user) -> DB.deleteOne(user))		
				.async( (user) -> {
					AdminMessagesClient.get().remoteDeleteUserInbox(name);
				});
	}

	@Override
	public Result<List<User>> searchUsers(String name, String pwd, String query) {
		Log.info( () -> "searchUsers : name = %s, pwd = %s, query = %s\n".formatted(name, pwd, query));

		if (badParams(name, pwd) )
			return error(BAD_REQUEST);

		var sqlExpr = "SELECT * FROM User u WHERE UPPER(u.name) LIKE '%%%s%%'".formatted(query.toUpperCase());
		
		return fetchUser(name, pwd )
				.then( () -> DB.select(sqlExpr, User.class))
				.mapValue( list -> list.stream().map(User::copyWithoutPassword).toList());
	}

	private boolean badUserInfo( User user) {
		return (user.getName() == null || user.getName().isEmpty()
				|| user.getPwd() == null || user.getPwd().isEmpty())
				|| user.getDisplayName() == null || user.getDisplayName().isEmpty()
				|| user.getDomain() == null || ! user.getDomain().equals( THIS_DOMAIN ) ;
	}


	private boolean badUpdateUserInfo( String name, String pwd, User info) {
		return (info.getName() != null && ! name.equals( info.getName()));
	}


	private Result<User> fetchUser(String name, String pwd) {
		return DB.getOne(name, User.class)
			.mapResult( user -> user.getPwd().equals(pwd) ? ok(user) : error(FORBIDDEN))
			.orElse( () -> error(FORBIDDEN ));
		
	}
	
	@Override
	public Result<Set<String>> checkUsers(Collection<String> addresses) {
		Log.info( () -> "checkUsers : addresses = %s\n".formatted(addresses));

		Set<String> res = new HashSet<>();
		for( var address : addresses )
			if( DB.getOne( getName( address ), User.class).error() == ErrorCode.NOT_FOUND )
				res.add(address );
		
		return ok(res);
	}

	
	public static synchronized JavaUsers getInstance() {
		if( instance == null )
			instance = new JavaUsers();
		return instance;
	}
	
	static JavaUsers instance;
}
