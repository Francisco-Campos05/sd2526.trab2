package sd2526.trab.impl.java.servers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import sd2526.trab.impl.discovery.Discovery;

public class ReplicationManager {

    private static final Logger Log = Logger.getLogger(ReplicationManager.class.getName());

    private final boolean isPrimary;
    private final AtomicLong seqNum = new AtomicLong(0);
    private final Map<String, Long> lastSidFromDomain = new ConcurrentHashMap<>();

    private final List<String> secondaryUris = new ArrayList<>();
    private volatile String primaryUri;

    public ReplicationManager(boolean isPrimary) {
        this.isPrimary = isPrimary;
    }

    public boolean isPrimary() { return isPrimary; }

    public long getCurrentVersion() { return seqNum.get(); }

    public long nextSeqNum() { return seqNum.incrementAndGet(); }

    public String getPrimaryUri() { return primaryUri; }

    public void setPrimaryUri(String uri) { this.primaryUri = uri; }

    public synchronized void addSecondary(String uri) {
        if (!secondaryUris.contains(uri)) {
            secondaryUris.add(uri);
            Log.info("Registered secondary: " + uri);
        }
    }

    public synchronized List<String> getSecondaryUris() {
        return new ArrayList<>(secondaryUris);
    }

    public void updateVersion(long newVersion) {
        long cur;
        do {
            cur = seqNum.get();
            if (newVersion <= cur) return;
        } while (!seqNum.compareAndSet(cur, newVersion));
    }

    public boolean checkAndUpdateSid(String sourceDomain, long sid) {
        return lastSidFromDomain.compute(sourceDomain, (k, last) -> {
            if (last != null && sid <= last) return last;
            return sid;
        }) == sid;
    }

    public void discoverPrimary(String domain) {
        new Thread(() -> {
            while (true) {
                try {
                    var uris = Discovery.getInstance().knownUrisOf("MessagesPrimary@" + domain, 1);
                    if (uris.length > 0) {
                        primaryUri = uris[0].toString();
                        Log.info("Discovered primary: " + primaryUri);
                        return;
                    }
                } catch (Exception e) {
                    Log.warning("Error discovering primary: " + e.getMessage());
                }
                sd2526.trab.impl.utils.Sleep.ms(1000);
            }
        }, "primary-discovery").start();
    }
}
