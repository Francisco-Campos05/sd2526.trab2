package sd2526.trab.impl.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

public class ZohoMailClient {

    private static final Logger Log = Logger.getLogger(ZohoMailClient.class.getName());

    // ---- Credentials (baked in) ----
    private static final String CLIENT_ID     = "1000.I1JQCAHZD3YQ230GLVNHVYI4NBMOTI";
    private static final String CLIENT_SECRET = "067d7bbd219cae50ea80e7749c093dc079d4563073";
    private static final String REFRESH_TOKEN = "1000.c1c4b0c562be5a06006d674d43d106bd.8c1ad57014b49665606a4f29d3054512";
    private static final String ACCOUNT_ID    = "8670264000000002002";
    public  static final String EMAIL         = "sd2526messages@zohomail.eu";
    private static final String API_BASE      = "https://mail.zoho.eu";
    private static final String TOKEN_URL     = "https://accounts.zoho.eu/oauth/v2/token";

    // Timeout for individual HTTP operations
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(8);

    /** Lightweight value object: Zoho's own IDs for a stored email. */
    public record ZohoEmail(String zohoId, String folderId, String subject) {}

    /**
     * SSLContext that trusts all certificates — needed because the Docker base
     * image's JVM truststore does not include the commercial CAs used by Zoho.
     * This is acceptable for an assignment context.
     */
    private static final SSLContext TRUST_ALL_CTX = buildTrustAllContext();

    private static SSLContext buildTrustAllContext() {
        try {
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, new TrustManager[]{
                new X509TrustManager() {
                    public void checkClientTrusted(X509Certificate[] c, String a) {}
                    public void checkServerTrusted(X509Certificate[] c, String a) {}
                    public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
                }
            }, new SecureRandom());
            return ctx;
        } catch (Exception e) {
            throw new RuntimeException("Failed to build trust-all SSLContext", e);
        }
    }

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(CONNECT_TIMEOUT)
            .sslContext(TRUST_ALL_CTX)
            .build();
    private final ObjectMapper mapper = new ObjectMapper();

    private String accessToken    = null;
    private long   tokenExpiresAt = 0L;

    // -------------------------------------------------------------------------
    // Token management
    // -------------------------------------------------------------------------

    public synchronized String getAccessToken() throws Exception {
        long now = System.currentTimeMillis();
        if (accessToken != null && now < tokenExpiresAt - 60_000L) {
            return accessToken;
        }

        String formBody = "grant_type=refresh_token"
                + "&client_id="     + URLEncoder.encode(CLIENT_ID,     StandardCharsets.UTF_8)
                + "&client_secret=" + URLEncoder.encode(CLIENT_SECRET, StandardCharsets.UTF_8)
                + "&refresh_token=" + URLEncoder.encode(REFRESH_TOKEN, StandardCharsets.UTF_8);

        var req = HttpRequest.newBuilder()
                .uri(URI.create(TOKEN_URL))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .timeout(REQUEST_TIMEOUT)
                .POST(HttpRequest.BodyPublishers.ofString(formBody))
                .build();

        var resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        var json = mapper.readTree(resp.body());

        if (!json.has("access_token")) {
            throw new RuntimeException("Token refresh failed (" + resp.statusCode() + "): " + resp.body());
        }
        accessToken = json.get("access_token").asText();
        long expiresIn = json.has("expires_in") ? json.get("expires_in").asLong() : 3600L;
        tokenExpiresAt = now + expiresIn * 1_000L;
        Log.info("ZohoMailClient: token refreshed, valid for " + expiresIn + "s");
        return accessToken;
    }

    // -------------------------------------------------------------------------
    // Mail operations
    // -------------------------------------------------------------------------

    /**
     * Lists up to 200 messages from the Sent folder (messages we created).
     */
    public List<ZohoEmail> listEmails() throws Exception {
        String token = getAccessToken();
        var req = HttpRequest.newBuilder()
                .uri(URI.create(API_BASE + "/api/accounts/" + ACCOUNT_ID
                        + "/messages/view?folder=Sent&limit=200&start=1"))
                .header("Authorization", "Zoho-oauthtoken " + token)
                .header("Accept", "application/json")
                .timeout(REQUEST_TIMEOUT)
                .GET()
                .build();

        var resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        Log.fine("listEmails (" + resp.statusCode() + "): "
                + resp.body().substring(0, Math.min(300, resp.body().length())));

        var json  = mapper.readTree(resp.body());
        var result = new ArrayList<ZohoEmail>();

        JsonNode data = json.get("data");
        if (data != null && data.isArray()) {
            for (JsonNode item : data) {
                String zohoId   = item.has("messageId") ? item.get("messageId").asText() : null;
                String folderId = item.has("folderId")  ? item.get("folderId").asText()  : null;
                String subject  = item.has("subject")   ? item.get("subject").asText()   : "";
                if (zohoId != null && folderId != null) {
                    result.add(new ZohoEmail(zohoId, folderId, subject));
                }
            }
        }
        Log.info("listEmails: found " + result.size() + " message(s)");
        return result;
    }

    /**
     * Returns the plain-text body of a specific stored email.
     */
    public String getEmailContent(String folderId, String zohoId) throws Exception {
        String token = getAccessToken();
        var req = HttpRequest.newBuilder()
                .uri(URI.create(API_BASE + "/api/accounts/" + ACCOUNT_ID
                        + "/folders/" + folderId + "/messages/" + zohoId + "/content"))
                .header("Authorization", "Zoho-oauthtoken " + token)
                .header("Accept", "application/json")
                .timeout(REQUEST_TIMEOUT)
                .GET()
                .build();

        var resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        var json = mapper.readTree(resp.body());

        JsonNode data = json.get("data");
        if (data == null) {
            throw new RuntimeException("No data in content response (" + resp.statusCode() + "): " + resp.body());
        }
        // Body text lives under data.content
        if (data.has("content")) return data.get("content").asText();
        return data.asText();
    }

    /**
     * Sends a self-addressed email and returns the Zoho IDs for the Sent-folder copy.
     * This is the canonical way to persist a message.
     */
    public ZohoEmail sendEmail(String subject, String body) throws Exception {
        String token = getAccessToken();

        var payload = mapper.createObjectNode();
        payload.put("fromAddress", EMAIL);
        payload.put("toAddress",   EMAIL);
        payload.put("subject",     subject);
        payload.put("content",     body);
        payload.put("mailFormat",  "plaintext");

        var req = HttpRequest.newBuilder()
                .uri(URI.create(API_BASE + "/api/accounts/" + ACCOUNT_ID + "/messages"))
                .header("Authorization", "Zoho-oauthtoken " + token)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .timeout(REQUEST_TIMEOUT)
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(payload)))
                .build();

        var resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200 && resp.statusCode() != 201) {
            throw new RuntimeException("sendEmail failed (" + resp.statusCode() + "): " + resp.body());
        }

        var json = mapper.readTree(resp.body());
        JsonNode data = json.get("data");
        if (data == null) {
            throw new RuntimeException("No data in sendEmail response: " + resp.body());
        }

        String zohoId   = data.has("messageId") ? data.get("messageId").asText() : null;
        String folderId = data.has("folderId")  ? data.get("folderId").asText()  : null;

        if (zohoId == null || folderId == null) {
            throw new RuntimeException("Missing messageId/folderId in sendEmail response: " + resp.body());
        }

        Log.info("sendEmail: stored '" + subject + "' zohoId=" + zohoId);
        return new ZohoEmail(zohoId, folderId, subject);
    }

    /**
     * Permanently deletes a stored email.
     */
    public void deleteEmail(String folderId, String zohoId) throws Exception {
        String token = getAccessToken();
        var req = HttpRequest.newBuilder()
                .uri(URI.create(API_BASE + "/api/accounts/" + ACCOUNT_ID
                        + "/folders/" + folderId + "/messages/" + zohoId))
                .header("Authorization", "Zoho-oauthtoken " + token)
                .header("Accept", "application/json")
                .timeout(REQUEST_TIMEOUT)
                .DELETE()
                .build();

        var resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200 && resp.statusCode() != 204) {
            Log.warning("deleteEmail unexpected response " + resp.statusCode() + ": " + resp.body());
        } else {
            Log.info("deleteEmail: removed zohoId=" + zohoId);
        }
    }
}
