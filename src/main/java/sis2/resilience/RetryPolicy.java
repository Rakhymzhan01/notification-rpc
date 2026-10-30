package sis2.resilience;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;

/**
 * Retry with exponential backoff + jitter, an overall time limit and an optional retry budget.
 * (Written by hand instead of Resilience4j so the project has no extra dependency; the structure is the same.)
 */
public final class RetryPolicy {

    public enum Jitter {
        /** delay = min(cap, base * mult^(n-1)); synchronized clients retry in lockstep, avoid in production. */
        NONE,
        /** delay = random(0, min(cap, base * mult^(n-1))); the default, spreads clients over time. */
        FULL,
        /** delay = min(cap, random(base, prevDelay * 3)); AWS "decorrelated jitter". */
        DECORRELATED
    }

    public record Config(int maxAttempts, Duration baseDelay, Duration maxDelay, double multiplier,
                         Jitter jitter, Duration totalTimeout) {

        public static Config defaults() {
            return new Config(4, Duration.ofMillis(100), Duration.ofSeconds(2), 2.0, Jitter.FULL, Duration.ofSeconds(3));
        }

        public static Config noRetry(Duration totalTimeout) {
            return new Config(1, Duration.ofMillis(1), Duration.ofMillis(1), 1.0, Jitter.NONE, totalTimeout);
        }
    }

    /** One attempt. {@code remaining} is the time left of the overall timeout; use it to size the per-attempt timeout. */
    @FunctionalInterface
    public interface Attempt<T> {
        T run(int attemptNumber, Duration remaining) throws Exception;
    }

    private final Config cfg;
    private final RetryBudget budget;

    public final LongAdder calls = new LongAdder();
    public final LongAdder attempts = new LongAdder();
    public final LongAdder retries = new LongAdder();
    public final LongAdder nonRetryable = new LongAdder();
    public final LongAdder exhausted = new LongAdder();
    public final LongAdder budgetDenied = new LongAdder();

    public RetryPolicy(Config cfg) {
        this(cfg, null);
    }

    public RetryPolicy(Config cfg, RetryBudget budget) {
        this.cfg = cfg;
        this.budget = budget;
    }

    public <T> T execute(Attempt<T> attempt, Predicate<Throwable> retryable) throws Exception {
        long start = System.nanoTime();
        long total = cfg.totalTimeout().toNanos();
        long prevDelay = cfg.baseDelay().toNanos();
        calls.increment();
        if (budget != null) budget.onRequest();

        for (int n = 1; ; n++) {
            long remaining = total - (System.nanoTime() - start);
            attempts.increment();
            try {
                return attempt.run(n, Duration.ofNanos(Math.max(remaining, 1)));
            } catch (Exception e) {
                if (!retryable.test(e)) {          // 4xx / INVALID_ARGUMENT / circuit open ...: retrying cannot help
                    nonRetryable.increment();
                    throw e;
                }
                if (n >= cfg.maxAttempts()) {
                    exhausted.increment();
                    throw e;
                }
                if (budget != null && !budget.tryAcquireRetry()) {
                    budgetDenied.increment();
                    throw e;
                }
                long delay = delayNanos(n, prevDelay);
                prevDelay = delay;
                long left = total - (System.nanoTime() - start);
                if (delay >= left) {               // no time left for another attempt
                    exhausted.increment();
                    throw e;
                }
                retries.increment();
                TimeUnit.NANOSECONDS.sleep(delay);
            }
        }
    }

    /** Delay before attempt n+1 (n = number of attempts already made). Public so tests/benchmarks can inspect the distribution. */
    public long delayNanos(int n, long prevDelayNanos) {
        long base = cfg.baseDelay().toNanos();
        long cap = cfg.maxDelay().toNanos();
        double exp = Math.min((double) cap, base * Math.pow(cfg.multiplier(), n - 1));
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        return switch (cfg.jitter()) {
            case NONE -> (long) exp;
            case FULL -> rnd.nextLong(0, (long) exp + 1);
            case DECORRELATED -> Math.min(cap, rnd.nextLong(base, Math.max(base + 1, prevDelayNanos * 3)));
        };
    }
}
