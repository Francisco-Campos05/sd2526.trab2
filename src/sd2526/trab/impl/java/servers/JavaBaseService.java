package sd2526.trab.impl.java.servers;

import sd2526.trab.api.User;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Result.ErrorCode;
import sd2526.trab.impl.java.clients.Clients;
import sd2526.trab.impl.utils.IP;

import java.util.function.Supplier;

public class JavaBaseService {

    protected final static String THIS_DOMAIN = IP.domain();
    protected final static String AT_THIS_DOMAIN = '@' + THIS_DOMAIN;

    protected boolean badParams(Object... params) {
        for (var p : params)
            if (p == null)
                return true;
        return false;
    }

    protected <T> Result<T> reTry(Supplier<Result<T>> func, int deadline) {
        Result<T> res;
        long T0 = System.currentTimeMillis();

        do {
            res = func.get();
        } while (res.error() == ErrorCode.TIMEOUT && (System.currentTimeMillis() - T0) < deadline);

        return res;
    }

    protected String getName(String address) {
        return address.split("@", 2)[0];
    }

    protected String getDomain(String address) {
        return address.split("@", 2)[1];
    }

    protected boolean isLocalDomain(String domain) {
        return domain.equals(THIS_DOMAIN);
    }

    protected boolean isLocalAddress(String address) {
        return address.endsWith(AT_THIS_DOMAIN);
    }

    protected Result<User> getUser(String address, String pwd) {
        try {
            var name = address.split("@", 2)[0];
            return Clients.UsersClient.get().getUser(name, pwd);
        } catch (Exception e) {
            e.printStackTrace();
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    }
}
