package sd2526.trab.impl.utils;

import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import javax.net.ssl.*;

public class TLSUtils {

    public static final String DEFAULT_PWD = "changeme";

    private static volatile SSLContext clientCtx;

    public static SSLContext serverContext(String keystoreFile, String pwd) {
        try {
            var ks = loadKeyStore(keystoreFile, pwd);
            var kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, pwd.toCharArray());
            var ctx = SSLContext.getInstance("TLS");
            ctx.init(kmf.getKeyManagers(), null, new SecureRandom());
            return ctx;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create server SSL context from: " + keystoreFile, e);
        }
    }

    public static synchronized SSLContext getClientContext() {
        if (clientCtx != null) return clientCtx;

        var tsFile = System.getenv("CLIENT_TRUSTSTORE");
        if (tsFile == null) tsFile = "client-truststore.ks";
        var tsPwd = System.getenv("CLIENT_TRUSTSTORE_PWD");
        if (tsPwd == null) tsPwd = DEFAULT_PWD;

        if (new File(tsFile).exists()) {
            clientCtx = truststoreContext(tsFile, tsPwd);
        } else {
            clientCtx = insecureContext();
        }
        return clientCtx;
    }

    public static boolean keystoreExists(String hostname) {
        return new File(hostname + ".ks").exists();
    }

    private static KeyStore loadKeyStore(String file, String pwd) throws Exception {
        var ks = KeyStore.getInstance("PKCS12");
        try (var fis = new FileInputStream(file)) {
            ks.load(fis, pwd.toCharArray());
        } catch (Exception e) {
            ks = KeyStore.getInstance("JKS");
            try (var fis = new FileInputStream(file)) {
                ks.load(fis, pwd.toCharArray());
            }
        }
        return ks;
    }

    private static SSLContext truststoreContext(String file, String pwd) {
        try {
            var ts = loadKeyStore(file, pwd);
            var tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ts);
            var ctx = SSLContext.getInstance("TLS");
            ctx.init(null, tmf.getTrustManagers(), new SecureRandom());
            return ctx;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create client SSL context from: " + file, e);
        }
    }

    @SuppressWarnings("all")
    public static SSLContext insecureContext() {
        try {
            TrustManager[] trustAll = {new X509TrustManager() {
                public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
                public void checkClientTrusted(X509Certificate[] c, String a) {}
                public void checkServerTrusted(X509Certificate[] c, String a) {}
            }};
            var ctx = SSLContext.getInstance("TLS");
            ctx.init(null, trustAll, new SecureRandom());
            return ctx;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
