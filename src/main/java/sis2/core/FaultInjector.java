package sis2.core;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test/demo switches that make the server misbehave, so retries, deadlines and idempotency can be shown on demand.
 * <ul>
 *   <li><b>failBefore</b> - request fails BEFORE the side effect (request effectively lost);</li>
 *   <li><b>failAfter</b> - the side effect IS committed, then the server answers UNAVAILABLE
 *       (response lost). This is the case idempotency exists for;</li>
 *   <li><b>delay</b> - slow server, used to trigger client timeouts.</li>
 * </ul>
 */
public final class FaultInjector {
    private final AtomicInteger failBeforeNext = new AtomicInteger();
    private final AtomicInteger failAfterNext = new AtomicInteger();
    private final AtomicInteger delayNextCalls = new AtomicInteger();
    private volatile long delayNextMillis;
    private volatile double failBeforeRate;
    private volatile double failAfterRate;
    private volatile long delayMillis;

    public FaultInjector failBeforeNext(int n) { failBeforeNext.set(n); return this; }
    public FaultInjector failAfterNext(int n) { failAfterNext.set(n); return this; }
    public FaultInjector delayNext(int calls, long millis) { delayNextMillis = millis; delayNextCalls.set(calls); return this; }
    public FaultInjector failBeforeRate(double p) { failBeforeRate = p; return this; }
    public FaultInjector failAfterRate(double p) { failAfterRate = p; return this; }
    public FaultInjector delayMillis(long ms) { delayMillis = ms; return this; }

    void delay() {
        long ms = delayMillis;
        if (delayNextCalls.get() > 0 && delayNextCalls.getAndDecrement() > 0) ms = delayNextMillis;
        if (ms > 0) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    void maybeFailBefore() {
        if (consume(failBeforeNext, failBeforeRate)) {
            throw new ServiceException(ServiceException.Kind.UNAVAILABLE, "injected failure before side effect");
        }
    }

    void maybeFailAfter() {
        if (consume(failAfterNext, failAfterRate)) {
            throw new ServiceException(ServiceException.Kind.UNAVAILABLE, "injected failure after side effect (response lost)");
        }
    }

    private static boolean consume(AtomicInteger counter, double rate) {
        if (counter.get() > 0 && counter.getAndDecrement() > 0) return true;
        return rate > 0 && ThreadLocalRandom.current().nextDouble() < rate;
    }
}
