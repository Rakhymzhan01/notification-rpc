package sis2.core;

/**
 * Outcome of PublishEvent (SIS #1 Ingest API). {@code accepted} mirrors the 202 semantics;
 * {@code replayed} is true when the answer came from the idempotency store.
 */
public record Result(String eventId, boolean accepted, boolean replayed) {
}
