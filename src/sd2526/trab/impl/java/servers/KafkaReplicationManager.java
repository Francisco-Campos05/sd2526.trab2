package sd2526.trab.impl.java.servers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class KafkaReplicationManager {

    private static final Logger Log = Logger.getLogger(KafkaReplicationManager.class.getName());

    public static final String BROKER = "kafka:9092";
    private static final String TOPIC_PREFIX = "Messages-";
    private static final long WRITE_TIMEOUT_MS = 15_000;
    private static final long READ_WAIT_MS     = 3_000;

    private final String topic;
    private final KafkaProducer<String, byte[]> producer;
    private final AtomicLong currentOffset = new AtomicLong(-1L);
    private final AtomicLong seqCounter    = new AtomicLong(0L);
    private final Map<String, Long> lastSidFromDomain = new ConcurrentHashMap<>();
    private final ObjectMapper mapper = new ObjectMapper();

    public KafkaReplicationManager(String domain) {
        this.topic = TOPIC_PREFIX + domain;
        createTopicIfAbsent();
        this.producer = createProducer();
        Log.info("KafkaReplicationManager ready, topic=" + topic);
    }

    // ---- Sequence / version ----

    public long nextSeqNum() { return seqCounter.incrementAndGet(); }

    public long getCurrentOffset() { return currentOffset.get(); }

    // ---- Publish ----

    public long publish(ReplicatedOperation op) throws Exception {
        byte[] bytes = mapper.writeValueAsBytes(op);
        RecordMetadata meta = producer.send(new ProducerRecord<>(topic, bytes)).get();
        return meta.offset();
    }

    // ---- Wait helpers ----

    public synchronized void waitForOffset(long targetOffset) throws InterruptedException {
        long deadline = System.currentTimeMillis() + WRITE_TIMEOUT_MS;
        while (currentOffset.get() < targetOffset) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0)
                throw new RuntimeException("Timeout waiting for Kafka offset " + targetOffset);
            wait(remaining);
        }
    }

    public synchronized void waitForVersion(long version) {
        long deadline = System.currentTimeMillis() + READ_WAIT_MS;
        while (currentOffset.get() < version) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) return;
            try { wait(remaining); } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); return;
            }
        }
    }

    // ---- Cross-domain SID dedup ----

    public boolean checkAndUpdateSid(String sourceDomain, long sid) {
        return lastSidFromDomain.compute(sourceDomain, (k, last) -> {
            if (last != null && sid <= last) return last;
            return sid;
        }) == sid;
    }

    // ---- Consumer ----

    public void startConsumer(String groupId, Consumer<ReplicatedOperation> handler) {
        Thread t = new Thread(() -> {
            KafkaConsumer<String, byte[]> consumer = createConsumer(groupId);
            consumer.subscribe(List.of(topic));
            Log.info("Kafka consumer started, groupId=" + groupId);
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    var records = consumer.poll(Duration.ofMillis(200));
                    for (var record : records) {
                        try {
                            var op = mapper.readValue(record.value(), ReplicatedOperation.class);
                            handler.accept(op);
                        } catch (Exception e) {
                            Log.warning("Error applying op at offset " + record.offset() + ": " + e.getMessage());
                        }
                        synchronized (KafkaReplicationManager.this) {
                            currentOffset.set(record.offset());
                            KafkaReplicationManager.this.notifyAll();
                        }
                    }
                    if (!records.isEmpty()) consumer.commitSync();
                } catch (Exception e) {
                    Log.warning("Kafka consumer error: " + e.getMessage());
                }
            }
        }, "kafka-consumer");
        t.setDaemon(true);
        t.start();
    }

    // ---- Kafka factories ----

    private KafkaProducer<String, byte[]> createProducer() {
        var p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "1");
        p.put(ProducerConfig.RETRIES_CONFIG, 5);
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000);
        return new KafkaProducer<>(p);
    }

    private KafkaConsumer<String, byte[]> createConsumer(String groupId) {
        var p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
        return new KafkaConsumer<>(p);
    }

    private void createTopicIfAbsent() {
        var p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        // Retry until Kafka is ready (it may not be up yet when server starts)
        for (int attempt = 0; attempt < 20; attempt++) {
            try (var admin = AdminClient.create(p)) {
                var existing = admin.listTopics().names().get();
                if (!existing.contains(topic)) {
                    admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
                    Log.info("Created Kafka topic: " + topic);
                } else {
                    Log.info("Kafka topic already exists: " + topic);
                }
                return;
            } catch (Exception e) {
                Log.warning("Kafka not ready (attempt " + attempt + "): " + e.getMessage());
                try { Thread.sleep(1000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); return; }
            }
        }
        Log.severe("Could not connect to Kafka after retries");
    }
}
