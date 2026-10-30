package sis2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import sis2.resilience.CircuitBreaker;
import sis2.resilience.RetryBudget;
import sis2.resilience.RetryPolicy;
import sis2.resilience.RetryPolicy.Config;
import sis2.resilience.RetryPolicy.Jitter;

class ResilienceTest {

    private static Config cfg(int attempts, Jitter j) {
        return new Config(attempts, Duration.ofMillis(5), Duration.ofMillis(40), 2.0, j, Duration.ofSeconds(2));
    }

    @Test
    void retriesTransientErrorsUntilSuccess() throws Exception {
        var p = new RetryPolicy(cfg(4, Jitter.FULL));
        AtomicInteger n = new AtomicInteger();
        String r = p.execute((a, rem) -> {
            if (n.incrementAndGet() < 3) throw new IOException("transient");
            return "ok";
        }, t -> t instanceof IOException);
        assertEquals("ok", r);
        assertEquals(3, p.attempts.sum());
        assertEquals(2, p.retries.sum());
    }

    @Test
    void doesNotRetryNonRetryableErrors() {
        var p = new RetryPolicy(cfg(4, Jitter.FULL));
        AtomicInteger n = new AtomicInteger();
        assertThrows(IllegalArgumentException.class, () -> p.execute((a, rem) -> {
            n.incrementAndGet();
            throw new IllegalArgumentException("validation");
        }, t -> t instanceof IOException));
        assertEquals(1, n.get());
        assertEquals(1, p.nonRetryable.sum());
    }

    @Test
    void givesUpAfterMaxAttempts() {
        var p = new RetryPolicy(cfg(3, Jitter.FULL));
        AtomicInteger n = new AtomicInteger();
        assertThrows(IOException.class, () -> p.execute((a, rem) -> {
            n.incrementAndGet();
            throw new IOException("down");
        }, t -> true));
        assertEquals(3, n.get());
        assertEquals(1, p.exhausted.sum());
    }

    @Test
    void stopsWhenTheOverallTimeoutIsUsedUp() {
        var c = new Config(100, Duration.ofMillis(50), Duration.ofMillis(50), 1.0, Jitter.NONE, Duration.ofMillis(120));
        var p = new RetryPolicy(c);
        long t0 = System.nanoTime();
        assertThrows(IOException.class, () -> p.execute((a, rem) -> { throw new IOException("down"); }, t -> true));
        long ms = (System.nanoTime() - t0) / 1_000_000;
        assertTrue(ms < 400, "must stop around the total timeout, took " + ms + " ms");
        assertTrue(p.attempts.sum() < 10);
    }

    @Test
    void remainingTimeShrinksAcrossAttempts() throws Exception {
        var c = new Config(3, Duration.ofMillis(30), Duration.ofMillis(30), 1.0, Jitter.NONE, Duration.ofMillis(500));
        var p = new RetryPolicy(c);
        var seen = new java.util.ArrayList<Long>();
        try {
            p.execute((a, rem) -> { seen.add(rem.toMillis()); throw new IOException("x"); }, t -> true);
        } catch (IOException expected) { }
        assertEquals(3, seen.size());
        assertTrue(seen.get(0) > seen.get(1) && seen.get(1) > seen.get(2), "deadline budget must shrink: " + seen);
    }

    @Test
    void fullJitterStaysWithinBoundsAndSpreadsClients() {
        var p = new RetryPolicy(new Config(5, Duration.ofMillis(100), Duration.ofSeconds(5), 2.0, Jitter.FULL, Duration.ofSeconds(30)));
        long max = Duration.ofMillis(400).toNanos();       // attempt n=3: base * 2^2
        java.util.Set<Long> distinct = new java.util.HashSet<>();
        for (int i = 0; i < 1000; i++) {
            long d = p.delayNanos(3, 0);
            assertTrue(d >= 0 && d <= max);
            distinct.add(d / 1_000_000);
        }
        assertTrue(distinct.size() > 100, "delays must be spread, got " + distinct.size() + " distinct ms values");
    }

    @Test
    void noJitterGivesExactExponentialSequence() {
        var p = new RetryPolicy(new Config(5, Duration.ofMillis(100), Duration.ofSeconds(1), 2.0, Jitter.NONE, Duration.ofSeconds(30)));
        assertEquals(100, p.delayNanos(1, 0) / 1_000_000);
        assertEquals(200, p.delayNanos(2, 0) / 1_000_000);
        assertEquals(400, p.delayNanos(3, 0) / 1_000_000);
        assertEquals(1000, p.delayNanos(6, 0) / 1_000_000, "capped at maxDelay");
    }

    @Test
    void retryBudgetStopsRetryStormDuringMassFailure() {
        var budget = new RetryBudget(0.1, 5);      // 5 free retries at start, then 1 retry per 10 requests
        var p = new RetryPolicy(cfg(3, Jitter.NONE), budget);
        for (int i = 0; i < 200; i++) {
            try {
                p.execute((a, rem) -> { throw new IOException("everything is down"); }, t -> true);
            } catch (Exception ignored) { }
        }
        // without a budget: 200 calls * 2 retries = 400 retries. With the budget: 5 + ~200*0.1 = ~25
        assertTrue(p.retries.sum() <= 30, "retries=" + p.retries.sum());
        assertTrue(p.budgetDenied.sum() > 100);
    }

    @Test
    void circuitBreakerOpensThenHalfOpensThenCloses() throws Exception {
        AtomicLong now = new AtomicLong();
        var cb = new CircuitBreaker(3, Duration.ofSeconds(10), 2, now::get);
        for (int i = 0; i < 3; i++) {
            assertThrows(IOException.class, () -> cb.call(() -> { throw new IOException("x"); }, t -> true));
        }
        assertEquals(CircuitBreaker.State.OPEN, cb.state());
        AtomicInteger reached = new AtomicInteger();
        assertThrows(CircuitBreaker.CircuitOpenException.class, () -> cb.call(() -> { reached.incrementAndGet(); return 1; }, t -> true));
        assertEquals(0, reached.get(), "open circuit fails fast without calling downstream");

        now.addAndGet(Duration.ofSeconds(11).toNanos());          // cooldown passed
        assertEquals(1, (int) cb.call(() -> 1, t -> true));
        assertEquals(CircuitBreaker.State.HALF_OPEN, cb.state());
        assertEquals(1, (int) cb.call(() -> 1, t -> true));       // 2nd probe succeeded
        assertEquals(CircuitBreaker.State.CLOSED, cb.state());
    }

    @Test
    void failedProbeReopensTheCircuit() throws Exception {
        AtomicLong now = new AtomicLong();
        var cb = new CircuitBreaker(1, Duration.ofSeconds(5), 1, now::get);
        assertThrows(IOException.class, () -> cb.call(() -> { throw new IOException(); }, t -> true));
        now.addAndGet(Duration.ofSeconds(6).toNanos());
        assertThrows(IOException.class, () -> cb.call(() -> { throw new IOException(); }, t -> true));
        assertEquals(CircuitBreaker.State.OPEN, cb.state());
        assertFalse(cb.state() == CircuitBreaker.State.CLOSED);
    }
}
