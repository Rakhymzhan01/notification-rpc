package sis2.core;

/** Common client interface so the benchmark can drive gRPC and REST identically. */
public interface IngestClient extends AutoCloseable {

    /**
     * Publish one event (SIS #1 Ingest API step).
     *
     * @param overrideKey optional explicit idempotency key (transport metadata / header);
     *                    when null, the server defaults to {@code cmd.eventId()}.
     */
    Result publishEvent(PublishEventCommand cmd, String overrideKey) throws Exception;

    @Override
    void close();
}
