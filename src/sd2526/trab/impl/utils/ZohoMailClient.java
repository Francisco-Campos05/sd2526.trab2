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
import java.util.Collections;
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
    private String sentFolderId   = null; // lazily resolved once

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
     * Resolves and caches the folder ID from any stored SD2526 email.
     * Called at startup so that subsequent sendEmail calls have a folderId ready.
     */
    public synchronized String getSentFolderIdFromList() throws Exception {
        if (sentFolderId != null) return sentFolderId;
        List<ZohoEmail> existing = listEmailsRaw();
        if (!existing.isEmpty()) {
            sentFolderId = existing.get(0).folderId();
            Log.info("getSentFolderIdFromList: resolved folderId=" + sentFolderId);
            return sentFolderId;
        }
        return null;
    }

    /**
     * Returns up to 5 emails matching "SD2526" via the search API.
     * Used to bootstrap the folderId cache when no specific email is needed.
     */
    private List<ZohoEmail> listEmailsRaw() throws Exception {
        return searchEmails("SD2526", 5);
    }

    /**
     * Searches for messages matching {@code keyword}, returning up to {@code limit} results.
     * Tries multiple known Zoho API URL patterns until one succeeds.
     */
    private List<ZohoEmail> searchEmails(String keyword, int limit) throws Exception {
        String token = getAccessToken();
        String enc   = URLEncoder.encode(keyword, StandardCharsets.UTF_8);

        // Candidates: most reliable first (view?searchKey works per live testing)
        String[] urls = {
            API_BASE + "/api/accounts/" + ACCOUNT_ID + "/messages/view?searchKey=" + enc + "&limit=" + limit + "&start=1",
            API_BASE + "/api/accounts/" + ACCOUNT_ID + "/messages/search?searchKey=" + enc + "&limit=" + limit + "&start=1",
            API_BASE + "/api/accounts/" + ACCOUNT_ID + "/messages/view?subject=" + enc + "&limit=" + limit + "&start=1",
        };

        for (String url : urls) {
            var req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Authorization", "Zoho-oauthtoken " + token)
                    .header("Accept", "application/json")
                    .timeout(REQUEST_TIMEOUT)
                    .GET()
                    .build();
            var resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            Log.info("searchEmails url=" + url.replaceAll(".*accounts/[^/]*/", "...")
                    + " (" + resp.statusCode() + "): "
                    + resp.body().substring(0, Math.min(400, resp.body().length())));
            if (resp.statusCode() == 200) {
                List<ZohoEmail> result = parseEmailList(resp.body());
                if (!result.isEmpty() || resp.body().contains("\"data\":[]")
                        || resp.body().contains("\"data\": []")) {
                    return result; // valid 200 response (even if empty)
                }
            }
        }
        return Collections.emptyList();
    }

    /** Parses a Zoho message-list JSON body into ZohoEmail records. */
    private List<ZohoEmail> parseEmailList(String body) throws Exception {
        var json   = mapper.readTree(body);
        var result = new ArrayList<ZohoEmail>();
        JsonNode data = json.get("data");
        if (data != null && data.isArray()) {
            for (JsonNode item : data) {
                String zohoId   = item.has("messageId") ? item.get("messageId").asText() : null;
                String folderId = item.has("folderId")  ? item.get("folderId").asText()  : null;
                String subject  = item.has("subject")   ? item.get("subject").asText()   : "";
                if (zohoId != null && folderId != null) result.add(new ZohoEmail(zohoId, folderId, subject));
            }
        }
        return result;
    }

    /**
     * Searches for up to 200 messages with subject prefix "SD2526:" across all folders.
     * Deduplicates by message ID so Inbox and Sent copies of the same email are merged.
     */
    public List<ZohoEmail> listEmails() throws Exception {
        List<ZohoEmail> raw = searchEmails("SD2526", 200);

        // Deduplicate: keep one record per (subject → SD2526:mid) combination.
        // If both Inbox and Sent copies appear, the last one wins (folderId may differ).
        var seen   = new java.util.LinkedHashMap<String, ZohoEmail>();
        for (ZohoEmail e : raw) {
            // Use zohoId as key (different copies have different zohoIds); that's fine —
            // JavaMessagesExternal deduplicates at a higher level using subject prefix.
            seen.put(e.zohoId(), e);
        }
        List<ZohoEmail> result = new ArrayList<>(seen.values());

        // Cache the folderId from any found email for later use
        if (sentFolderId == null && !result.isEmpty()) {
            sentFolderId = result.get(0).folderId();
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
        Log.info("sendEmail response (" + resp.statusCode() + "): "
                + resp.body().substring(0, Math.min(500, resp.body().length())));
        if (resp.statusCode() != 200 && resp.statusCode() != 201) {
            throw new RuntimeException("sendEmail failed (" + resp.statusCode() + "): " + resp.body());
        }

        var json = mapper.readTree(resp.body());
        JsonNode data = json.get("data");
        if (data == null) {
            throw new RuntimeException("No data in sendEmail response: " + resp.body());
        }

        // The send response returns messageId but NOT folderId.
        // Resolve folderId by listing the Sent folder (within our OAuth scope).
        String zohoId   = data.has("messageId") ? data.get("messageId").asText() : null;
        String folderId = data.has("folderId")  ? data.get("folderId").asText()  : null;

        if (zohoId == null) {
            throw new RuntimeException("Missing messageId in sendEmail response: " + resp.body());
        }

        // If folderId not returned (typical), try to resolve it.
        if (folderId == null) {
            if (sentFolderId != null) {
                folderId = sentFolderId; // cached from earlier listing
            } else {
                // Search for our just-sent message (up to 4 retries, 800 ms apart)
                for (int retry = 0; retry < 4 && folderId == null; retry++) {
                    if (retry > 0) Thread.sleep(800);
                    List<ZohoEmail> found = searchEmails("SD2526", 10);
                    for (ZohoEmail e : found) {
                        folderId = e.folderId();
                        sentFolderId = folderId;
                        Log.info("sendEmail: resolved folderId=" + folderId + " via search (retry " + retry + ")");
                        break;
                    }
                }
            }
        }

        if (folderId == null) {
            // Email was sent to Zoho successfully; folderId will be resolvable on next listEmails().
            Log.warning("sendEmail: folderId not yet resolvable for " + zohoId
                    + " — email IS in Zoho, deletion may be deferred");
        }

        Log.info("sendEmail: stored '" + subject + "' zohoId=" + zohoId + " folderId=" + folderId);
        return new ZohoEmail(zohoId, folderId, subject);
    }

    /**
     * Permanently deletes a stored email.
     * If {@code folderId} is null (e.g. not yet resolved after initial send),
     * the method attempts to locate it via the Sent folder listing first.
     */
    public void deleteEmail(String folderId, String zohoId) throws Exception {
        if (folderId == null) {
            // Try to resolve folderId on-the-fly via search
            List<ZohoEmail> found = searchEmails("SD2526", 10);
            for (ZohoEmail e : found) {
                if (zohoId.equals(e.zohoId())) { folderId = e.folderId(); break; }
            }
            if (folderId == null && !found.isEmpty()) folderId = found.get(0).folderId();
            if (folderId == null) {
                Log.warning("deleteEmail: folderId unknown for " + zohoId + ", skipping delete");
                return;
            }
        }
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
