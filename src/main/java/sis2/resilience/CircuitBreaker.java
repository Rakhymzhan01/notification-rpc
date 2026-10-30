package sis2.resilience;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

/**
 * CLOSED -> (N consecutive failures) -> OPEN -> (cooldown) -> HALF_OPEN -> (K probe successes) -> CLOSED.
 * While OPEN every call fails immediately, so a dead downstream is not hammered and callers do not wait for timeouts.
 */
public final class CircuitBreaker {

    public enum State { CLOSED, OPEN, HALF_OPEN }

    public static final class CircuitOpenException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        public CircuitOpenException() {
            super("circuit breaker is open");
        }
    }

    private final int failureThreshold;
    private final long openNanos;
    private final int probes;
    private final LongSupplier clock;

    private State state = State.CLOSED;
    private int consecutiveFailures;
    private long openedAt;
    private int probesInFlight;
    private int probeSuccesses;

    public CircuitBreaker(int failureThreshold, Duration openDuration, int probes) {
        this(failureThreshold, openDuration, probes, System::nanoTime);
    }

    public CircuitBreaker(int failureThreshold, Duration openDuration, int probes, LongSupplier clock) {
        this.failureThreshold = failureThreshold;
        this.openNanos = openDuration.toNanos();
        this.probes = probes;
        this.clock = clock;
    }

    /** @param isFailure which exceptions mean "downstream is unhealthy" (a 400 means it is healthy, so it must not trip the breaker) */
    public <T> T call(Callable<T> action, Predicate<Throwable> isFailure) throws Exception {
        acquire();
        try {
            T r = action.call();
            onSuccess();
            return r;
        } catch (Exception e) {
            if (isFailure.test(e)) onFailure(); else onSuccess();
            throw e;
        }
    }

    public synchronized State state() {
        return state;
    }

    private synchronized void acquire() {
        if (state == State.OPEN) {
            if (clock.getAsLong() - openedAt >= openNanos) {
                state = State.HALF_OPEN;
                probesInFlight = 0;
                probeSuccesses = 0;
            } else {
                throw new CircuitOpenException();
            }
        }
        if (state == State.HALF_OPEN) {
            if (probesInFlight >= probes) throw new CircuitOpenException();
            probesInFlight++;
        }
    }

    private synchronized void onSuccess() {
        if (state == State.HALF_OPEN) {
            probesInFlight--;
            if (++probeSuccesses >= probes) {
                state = State.CLOSED;
                consecutiveFailures = 0;
            }
        } else if (state == State.CLOSED) {
            consecutiveFailures = 0;
        }
    }

    private synchronized void onFailure() {
        if (state == State.HALF_OPEN) {
            trip();
        } else if (state == State.CLOSED && ++consecutiveFailures >= failureThreshold) {
            trip();
        }
    }

    private void trip() {
        state = State.OPEN;
        openedAt = clock.getAsLong();
        probesInFlight = 0;
    }
}
