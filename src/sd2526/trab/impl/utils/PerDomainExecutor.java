package sd2526.trab.impl.utils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * One single-threaded executor per domain key, so jobs for the same domain run
 * serially while different domains can proceed in parallel.
 */
public class PerDomainExecutor {

    private final ConcurrentHashMap<String, ExecutorService> executors = new ConcurrentHashMap<>();

    public void submit(String domain, Runnable job) {
        ExecutorService executor = executors.computeIfAbsent(
                domain,
                d -> Executors.newSingleThreadExecutor(r -> {
                    Thread t = new Thread(r);
                    t.setUncaughtExceptionHandler((thr, ex) -> ex.printStackTrace());
                    return t;
                })
        );
        executor.submit(job);
    }
}
