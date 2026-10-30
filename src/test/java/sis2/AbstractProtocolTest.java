package sis2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sis2.core.ClientOptions;
import sis2.core.FaultInjector;
import sis2.core.InMemoryIdempotencyStore;
import sis2.core.IngestClient;
import sis2.core.IngestCore;
import sis2.core.PublishEventCommand;
import sis2.core.Result;
import sis2.resilience.RetryPolicy;
import sis2.resilience.RetryPolicy.Config;
import sis2.resilience.RetryPolicy.Jitter;

/** The same behavioural contract is checked for BOTH transports. */
abstract class AbstractProtocolTest {

    protected FaultInjector faults;
    protected IngestCore core;
    protected IngestClient client;
    private AutoCloseable server;

    /** Starts the server on an ephemeral port and returns it; subclasses also remember the port. */
    protected abstract AutoCloseable startServer(IngestCore core) throws Exception;

    protected abstract IngestClient newClient(ClientOptions opts);

    protected abstract RetryPolicy policy();

    static ClientOptions fast() {
        return new ClientOptions(Duration.ofSeconds(1), Duration.ofMillis(400), false,
                new Config(4, Duration.ofMillis(10), Duration.ofMillis(50), 2.0, Jitter.FULL, Duration.ofSeconds(5)), null, null);
    }

    /** One event, no eventId in the body - the server will fill one in (or the override key wins). */
    static PublishEventCommand cmd() {
        return new PublishEventCommand(null, "user-1", "order.created", "demo payload");
    }

    @BeforeEach
    void setUp() throws Exception {
        faults = new FaultInjector();
        core = new IngestCore(new InMemoryIdempotencyStore(Duration.ofHours(1)), faults);
        server = startServer(core);
        client = newClient(fast());
    }

    @AfterEach
    void tearDown() throws Exception {
        client.close();
        server.close();
    }

    @Test
    void happyPath() throws Exception {
        Result r = client.publishEvent(cmd(), UUID.randomUUID().toString());
        assertTrue(r.accepted());
        assertFalse(r.replayed());
        assertEquals(1, core.kafkaWrites());
    }

    @Test
    void sameKeyTwice_oneKafkaWrite_secondIsReplayed() throws Exception {
        String key = UUID.randomUUID().toString();
        Result a = client.publishEvent(cmd(), key);
        Result b = client.publishEvent(cmd(), key);
        assertEquals(a.eventId(), b.eventId());
        assertTrue(b.replayed());
        assertEquals(1, core.kafkaWrites());
    }

    @Test
    void responseLost_retryWithKey_kafkaWriteHappensExactlyOnce() throws Exception {
        faults.failAfterNext(1);        // server commits, then answers UNAVAILABLE / 503
        Result r = client.publishEvent(cmd(), UUID.randomUUID().toString());
        assertEquals(1, core.kafkaWrites(), "at-least-once delivery + idempotent handler = effectively once");
        assertTrue(r.replayed(), "the retry was answered from the idempotency store");
        assertEquals(2, policy().attempts.sum());
        assertEquals(1, policy().retries.sum());
    }

    @Test
    void responseLost_retryWithoutKey_duplicatesTheKafkaWrite() throws Exception {
        faults.failAfterNext(1);
        // No event_id in the body AND no override key: the server generates a fresh UUID per call,
        // so the retry has a DIFFERENT idempotency key and the Kafka write runs twice.
        // This is the test that shows WHY the producer MUST send a stable event_id.
        client.publishEvent(cmd(), null);
        assertEquals(2, core.kafkaWrites(), "without an idempotency key a retry executes the Kafka write again");
    }

    @Test
    void requestLost_isRetriedWithBackoffAndSucceeds() throws Exception {
        faults.failBeforeNext(2);
        client.publishEvent(cmd(), UUID.randomUUID().toString());
        assertEquals(3, policy().attempts.sum());
        assertEquals(1, core.kafkaWrites());
    }

    @Test
    void persistentFailure_stopsAfterMaxAttempts() {
        faults.failBeforeRate(1.0);
        assertThrows(Exception.class, () -> client.publishEvent(cmd(), UUID.randomUUID().toString()));
        assertEquals(4, policy().attempts.sum());
        assertEquals(1, policy().exhausted.sum());
        assertEquals(0, core.kafkaWrites());
    }

    @Test
    void validationError_isNotRetried() {
        PublishEventCommand bad = new PublishEventCommand(null, "", "order.created", "x");   // blank user_id
        assertThrows(Exception.class, () -> client.publishEvent(bad, UUID.randomUUID().toString()));
        assertEquals(1, policy().attempts.sum(), "INVALID_ARGUMENT / HTTP 400 must not be retried");
        assertEquals(1, policy().nonRetryable.sum());
    }

    @Test
    void sameKeyDifferentPayload_isRejectedAndNotRetried() throws Exception {
        String key = UUID.randomUUID().toString();
        client.publishEvent(cmd(), key);
        PublishEventCommand other = new PublishEventCommand(null, "user-999", "order.cancelled", "different");
        assertThrows(Exception.class, () -> client.publishEvent(other, key));
        assertEquals(2, policy().attempts.sum(), "1 for the first call + exactly 1 (no retry) for the rejected one");
        assertEquals(1, core.kafkaWrites());
    }

    @Test
    void slowFirstAttempt_timesOut_retrySucceeds_stillOneKafkaWrite() throws Exception {
        faults.delayNext(1, 900);       // first attempt takes 900 ms, per-attempt timeout is 400 ms
        Result r = client.publishEvent(cmd(), UUID.randomUUID().toString());
        assertTrue(r.accepted());
        assertTrue(policy().attempts.sum() >= 2);
        Thread.sleep(700);              // let the abandoned first attempt finish on the server
        assertEquals(1, core.kafkaWrites(), "the abandoned first attempt must not duplicate the Kafka write");
    }

    // ---- Main-path tests: event_id in the body IS the idempotency key, no override header ----

    @Test
    void responseLost_retryWithEventIdInBody_kafkaWriteHappensExactlyOnce() throws Exception {
        String eventId = UUID.randomUUID().toString();
        PublishEventCommand c = new PublishEventCommand(eventId, "user-1", "order.created", "demo payload");
        faults.failAfterNext(1);                                   // server commits Kafka write, then "loses" the answer
        Result r = client.publishEvent(c, null);                    // null overrideKey -> body event_id IS the key
        assertEquals(1, core.kafkaWrites(), "at-least-once + idempotent handler = effectively once (body event_id key)");
        assertTrue(r.replayed());
        assertEquals(eventId, r.eventId());
    }

    @Test
    void sameEventIdTwice_secondIsReplayed() throws Exception {
        String eventId = UUID.randomUUID().toString();
        PublishEventCommand c = new PublishEventCommand(eventId, "user-1", "order.created", "demo payload");
        Result a = client.publishEvent(c, null);
        Result b = client.publishEvent(c, null);
        assertEquals(eventId, a.eventId());
        assertEquals(eventId, b.eventId());
        assertTrue(b.replayed());
        assertEquals(1, core.kafkaWrites());
    }

    @Test
    void sameEventIdDifferentPayload_isRejected() throws Exception {
        String eventId = UUID.randomUUID().toString();
        PublishEventCommand first = new PublishEventCommand(eventId, "user-1", "order.created", "first body");
        PublishEventCommand second = new PublishEventCommand(eventId, "user-1", "order.created", "second body");
        client.publishEvent(first, null);
        assertThrows(Exception.class, () -> client.publishEvent(second, null));
        assertEquals(2, policy().attempts.sum(), "rejection must not trigger a retry");
        assertEquals(1, core.kafkaWrites());
    }

    @Test
    void overrideKeyWinsOverBodyEventId() throws Exception {
        // Different event_ids in the body, but the same override header -> dedup to ONE Kafka write.
        String overrideKey = UUID.randomUUID().toString();
        PublishEventCommand cA = new PublishEventCommand("event-A", "user-1", "order.created", "payload");
        PublishEventCommand cB = new PublishEventCommand("event-B", "user-1", "order.created", "payload");
        Result first = client.publishEvent(cA, overrideKey);
        Result second = client.publishEvent(cB, overrideKey);
        assertEquals(1, core.kafkaWrites(), "override key takes precedence over body event_id");
        assertTrue(second.replayed(), "second call is replayed because the override key is the same");
        assertEquals(first.eventId(), second.eventId(),
                "replay returns the cached response (first call's event_id, not the one in the second request body)");
    }

    @Test
    void gzipRoundTrip() throws Exception {
        try (IngestClient gz = newClient(fast().withGzip(true))) {
            Result r = gz.publishEvent(
                    new PublishEventCommand(null, "user-2", "order.updated", "x".repeat(5000)),
                    UUID.randomUUID().toString());
            assertTrue(r.accepted());
        }
    }
}
