package sis2.core;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * (idempotency_key -> fingerprint + result) with TTL.
 * Concurrent duplicates wait for the first execution instead of running the side effect twice.
 * Expired entries are dropped lazily on access and by a background sweeper, so memory does not leak.
 */
public final class InMemoryIdempotencyStore implements IdempotencyStore, AutoCloseable {

    private static final class Entry {
        final String fingerprint;
        final CompletableFuture<Result> future = new CompletableFuture<>();
        /** Long.MAX_VALUE while the first execution is still in flight. */
        volatile long expiresAtNanos = Long.MAX_VALUE;

        Entry(String fingerprint) {
            this.fingerprint = fingerprint;
        }
    }

    private final ConcurrentHashMap<String, Entry> map = new ConcurrentHashMap<>();
    private final long ttlNanos;
    private final LongSupplier clockNanos;
    private final LongAdder replays = new LongAdder();
    private ScheduledExecutorService sweeper;

    public InMemoryIdempotencyStore(Duration ttl) {
        this(ttl, System::nanoTime);
    }

    /** The clock is injectable so tests can move time without sleeping. */
    public InMemoryIdempotencyStore(Duration ttl, LongSupplier clockNanos) {
        this.ttlNanos = ttl.toNanos();
        this.clockNanos = clockNanos;
    }

    public InMemoryIdempotencyStore startSweeper(Duration every) {
        sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "idempotency-sweeper");
            t.setDaemon(true);
            return t;
        });
        sweeper.scheduleWithFixedDelay(this::evictExpired, every.toMillis(), every.toMillis(), TimeUnit.MILLISECONDS);
        return this;
    }

    @Override
    public Result execute(String key, String fingerprint, Supplier<Result> action) {
        while (true) {
            Entry mine = new Entry(fingerprint);
            Entry existing = map.putIfAbsent(key, mine);
            if (existing != null && existing.expiresAtNanos <= clockNanos.getAsLong()) {
                // expired: try to take its place; if somebody else did first, start over
                if (map.replace(key, existing, mine)) {
                    existing = null;
                } else {
                    continue;
                }
            }
            if (existing == null) {
                return runFirst(key, mine, action);
            }
            if (!existing.fingerprint.equals(fingerprint)) {
                throw new ServiceException(ServiceException.Kind.KEY_REUSED,
                        "idempotency key was already used with a different request");
            }
            try {
                Result r = existing.future.get(); // waits if the first execution is still running
                replays.increment();
                return new Result(r.eventId(), r.accepted(), true);
            } catch (ExecutionException e) {
                // the first execution failed and was not cached: try to execute ourselves
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ServiceException(ServiceException.Kind.CANCELLED, "interrupted while waiting for first execution");
            }
        }
    }

    private Result runFirst(String key, Entry mine, Supplier<Result> action) {
        try {
            Result r = action.get();
            mine.expiresAtNanos = clockNanos.getAsLong() + ttlNanos;
            mine.future.complete(r);
            return r;
        } catch (RuntimeException | Error e) {
            map.remove(key, mine);                 // never cache failures
            mine.future.completeExceptionally(e);
            throw e;
        }
    }

    /** Removes expired entries; returns how many were removed. */
    public int evictExpired() {
        long now = clockNanos.getAsLong();
        int before = map.size();
        map.entrySet().removeIf(e -> e.getValue().expiresAtNanos <= now);
        return before - map.size();
    }

    @Override
    public int size() {
        return map.size();
    }

    public long replays() {
        return replays.sum();
    }

    @Override
    public void close() {
        if (sweeper != null) sweeper.shutdownNow();
    }
}
