package sis2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import sis2.core.InMemoryIdempotencyStore;
import sis2.core.Result;
import sis2.core.ServiceException;

class IdempotencyStoreTest {

    private static Result r() {
        return new Result("id-" + System.nanoTime(), true, false);
    }

    @Test
    void sameKeyRunsActionOnce_andSecondCallIsMarkedReplayed() {
        var store = new InMemoryIdempotencyStore(Duration.ofHours(24));
        AtomicInteger runs = new AtomicInteger();
        Result a = store.execute("k1", "fp", () -> { runs.incrementAndGet(); return r(); });
        Result b = store.execute("k1", "fp", () -> { runs.incrementAndGet(); return r(); });
        assertEquals(1, runs.get());
        assertEquals(a.eventId(), b.eventId());
        assertFalse(a.replayed());
        assertTrue(b.replayed());
    }

    @Test
    void concurrentDuplicatesWaitForTheFirstExecution() throws Exception {
        var store = new InMemoryIdempotencyStore(Duration.ofHours(1));
        AtomicInteger runs = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(32);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<Result>> fs = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            fs.add(pool.submit(() -> {
                go.await();
                return store.execute("same", "fp", () -> {
                    runs.incrementAndGet();
                    try { Thread.sleep(100); } catch (InterruptedException ignored) { }
                    return r();
                });
            }));
        }
        go.countDown();
        String id = fs.get(0).get().eventId();
        for (Future<Result> f : fs) assertEquals(id, f.get().eventId());
        assertEquals(1, runs.get(), "side effect must happen exactly once for 32 concurrent duplicates");
        pool.shutdownNow();
    }

    @Test
    void sameKeyDifferentPayloadIsRejected() {
        var store = new InMemoryIdempotencyStore(Duration.ofHours(1));
        store.execute("k", "fp-A", IdempotencyStoreTest::r);
        ServiceException e = assertThrows(ServiceException.class, () -> store.execute("k", "fp-B", IdempotencyStoreTest::r));
        assertEquals(ServiceException.Kind.KEY_REUSED, e.kind());
    }

    @Test
    void failuresAreNotCached_soARetryCanRun() {
        var store = new InMemoryIdempotencyStore(Duration.ofHours(1));
        AtomicInteger runs = new AtomicInteger();
        assertThrows(ServiceException.class, () -> store.execute("k", "fp", () -> {
            runs.incrementAndGet();
            throw new ServiceException(ServiceException.Kind.UNAVAILABLE, "boom");
        }));
        Result ok = store.execute("k", "fp", () -> { runs.incrementAndGet(); return r(); });
        assertEquals(2, runs.get());
        assertFalse(ok.replayed());
    }

    @Test
    void entriesExpireAfterTtl_andAreEvicted() {
        AtomicLong now = new AtomicLong();
        var store = new InMemoryIdempotencyStore(Duration.ofSeconds(10), now::get);
        AtomicInteger runs = new AtomicInteger();
        store.execute("k", "fp", () -> { runs.incrementAndGet(); return r(); });
        store.execute("other", "fp", () -> { runs.incrementAndGet(); return r(); });
        assertEquals(2, store.size());

        now.addAndGet(Duration.ofSeconds(9).toNanos());
        store.execute("k", "fp", () -> { runs.incrementAndGet(); return r(); });
        assertEquals(2, runs.get(), "still inside TTL: replayed");

        now.addAndGet(Duration.ofSeconds(2).toNanos());       // now past the TTL
        assertEquals(2, store.evictExpired(), "sweeper drops expired entries (no memory leak)");
        assertEquals(0, store.size());
        store.execute("k", "fp", () -> { runs.incrementAndGet(); return r(); });
        assertEquals(3, runs.get(), "after TTL the key is new again");
    }
}
