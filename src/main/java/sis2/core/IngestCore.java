package sis2.core;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * The Ingest API business logic from SIS #1: validate, (optionally) deduplicate, write to Kafka.
 * Shared by the gRPC server and the REST server so that the benchmark compares protocols, not handlers.
 *
 * <p>The Kafka write is simulated by {@link #publishToKafka(PublishEventCommand)} - a counter increment.
 * In the real system this is Producer.send with acks=all, keyed by user_id.
 */
public final class IngestCore {

    private final IdempotencyStore store;
    private final FaultInjector faults;
    /** How many times the simulated "write to Kafka" really happened. The idempotency tests assert on this. */
    private final AtomicLong kafkaWrites = new AtomicLong();

    public IngestCore(IdempotencyStore store, FaultInjector faults) {
        this.store = store;
        this.faults = faults;
    }

    /**
     * Accept an event (semantics of HTTP 202 / gRPC OK). Writes to Kafka exactly once per
     * effective idempotency key (see below), otherwise once per call.
     *
     * @param overrideKey optional explicit idempotency key from transport metadata
     *                    (gRPC metadata "idempotency-key" / HTTP "Idempotency-Key" header).
     *                    When null/blank, the key defaults to {@code cmd.eventId()}.
     * @param cmd         the event; {@code cmd.eventId()} may be null/blank (producer did not supply one).
     * @param cancelled   true once the caller's deadline passed or the call was cancelled
     *                    (gRPC: Context.isCancelled(); REST has no such signal).
     */
    public Result publishEvent(String overrideKey, PublishEventCommand cmd, BooleanSupplier cancelled) {
        cmd.validate();
        faults.delay();
        if (cancelled.getAsBoolean()) {
            // deadline propagation: nobody is waiting for the answer, so do not write to Kafka
            throw new ServiceException(ServiceException.Kind.CANCELLED, "caller gave up before the work started");
        }
        faults.maybeFailBefore();

        // Effective event_id: what the producer sent; if blank, server generates one on the fly.
        String effectiveEventId = (cmd.eventId() == null || cmd.eventId().isBlank())
                ? UUID.randomUUID().toString()
                : cmd.eventId();

        // Effective idempotency key: explicit override wins, otherwise event_id from the body.
        // If BOTH are missing (producer did not send event_id AND no header), there is no dedup,
        // so a retry will produce a duplicate Kafka write - that is why the producer MUST send event_id.
        String key = (overrideKey != null && !overrideKey.isBlank())
                ? overrideKey
                : (cmd.eventId() != null && !cmd.eventId().isBlank() ? cmd.eventId() : null);

        PublishEventCommand effective = new PublishEventCommand(
                effectiveEventId, cmd.userId(), cmd.type(), cmd.payload());

        Result result;
        if (key == null) {
            result = publishToKafka(effective);
        } else {
            result = store.execute(key, effective.fingerprint(), () -> publishToKafka(effective));
        }

        faults.maybeFailAfter(); // response lost AFTER the Kafka write: the retry must not duplicate it
        return result;
    }

    /** Simulated Producer.send(topic=events, key=user_id, value=event) from SIS #1 (acks=all). */
    private Result publishToKafka(PublishEventCommand cmd) {
        kafkaWrites.incrementAndGet();
        return new Result(cmd.eventId(), true, false);
    }

    /** How many times the simulated Kafka write really happened. */
    public long kafkaWrites() {
        return kafkaWrites.get();
    }

    public IdempotencyStore store() {
        return store;
    }
}
