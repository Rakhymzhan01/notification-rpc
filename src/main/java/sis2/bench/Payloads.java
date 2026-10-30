package sis2.bench;

import java.util.Random;
import sis2.core.PublishEventCommand;

/** Deterministic benchmark payloads (fixed seed => both protocols send exactly the same data). */
public final class Payloads {
    private Payloads() {
    }

    private static final String[] WORDS = {
            "your", "order", "has", "been", "shipped", "and", "will", "arrive", "tomorrow", "please", "confirm",
            "payment", "account", "balance", "transfer", "received", "limit", "approved", "new", "offer", "card",
            "bank", "deposit", "credit", "loan", "reminder", "due", "date", "security", "code", "login", "device",
            "thank", "you", "for", "using", "our", "service", "details", "available", "in", "the", "app", "today"};

    /**
     * One event, payload of roughly {@code payloadBytes} bytes of English-like text (compresses ~2-3x with gzip).
     * eventId is null here so the server generates one; the LoadGenerator overrides it per call via the header
     * so the idempotency store is actually exercised.
     */
    public static PublishEventCommand text(int payloadBytes) {
        Random rnd = new Random(42);
        StringBuilder sb = new StringBuilder(payloadBytes + 16);
        while (sb.length() < payloadBytes) {
            if (sb.length() > 0) sb.append(' ');
            sb.append(WORDS[rnd.nextInt(WORDS.length)]);
        }
        sb.setLength(payloadBytes);
        return new PublishEventCommand(null, "user-1234567", "order.shipped", sb.toString());
    }

    /**
     * Fan-out mode was removed: the SIS #1 Ingest API accepts one event per call. Only "text" remains.
     * The parameter is kept so the LoadGenerator CLI and the CSV format stay compatible.
     */
    public static PublishEventCommand make(String mode, int size) {
        if (!"text".equals(mode)) {
            throw new IllegalArgumentException("mode must be 'text' (fan-out removed - Ingest API takes one event per call)");
        }
        return text(size);
    }
}
