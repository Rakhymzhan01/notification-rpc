package sis2.bench;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;
import sis2.core.ClientOptions;
import sis2.core.IngestClient;
import sis2.core.PublishEventCommand;
import sis2.grpc.GrpcIngestClient;
import sis2.rest.RestIngestClient;

/**
 * Open-loop, fixed-rate load generator (separate JVM from the server).
 * <ul>
 *   <li>request i has an INTENDED start time t0 + i/rps; latency = completion - intended start, so when the
 *       server stalls the queueing delay is counted (no coordinated omission);</li>
 *   <li>the first --warmup seconds run at the same rate but are not recorded (JIT, connection pool, caches);</li>
 *   <li>percentiles come from HdrHistogram. "service" latency (from the actual publishEvent call) is printed too,
 *       to show how much a closed-loop tool would hide.</li>
 * </ul>
 * Usage: --proto grpc|grpc-gzip|rest|rest-gzip --port N --rps N --duration S --warmup S --size N --threads N
 * (fan-out mode was removed: the Ingest API takes one event per call.)
 */
public final class LoadGenerator {

    /** mode column is always "text" after fan-out removal; kept so existing CSV consumers keep working. */
    public static final String CSV_HEADER =
            "proto,mode,size,rps_target,rps_achieved,ok,errors,p50_us,p90_us,p95_us,p99_us,p999_us,max_us,svc_p50_us,svc_p99_us";

    public static void main(String[] args) throws Exception {
        Map<String, String> a = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) a.put(args[i].replaceFirst("^--", ""), args[i + 1]);

        String proto = a.getOrDefault("proto", "grpc");
        String host = a.getOrDefault("host", "127.0.0.1");
        int port = Integer.parseInt(a.getOrDefault("port", "50051"));
        int rps = Integer.parseInt(a.getOrDefault("rps", "1000"));
        int duration = Integer.parseInt(a.getOrDefault("duration", "10"));
        int warmup = Integer.parseInt(a.getOrDefault("warmup", "5"));
        int size = Integer.parseInt(a.getOrDefault("size", "1024"));
        int threads = Integer.parseInt(a.getOrDefault("threads", "128"));

        boolean gzip = proto.endsWith("-gzip");
        ClientOptions opts = ClientOptions.benchmark(gzip);
        try (IngestClient client = proto.startsWith("grpc")
                ? new GrpcIngestClient(host, port, opts)
                : new RestIngestClient(host, port, opts)) {
            PublishEventCommand cmd = Payloads.text(size);
            run(client, cmd, rps, warmup, threads, false);                    // warm-up, discarded
            Stats s = run(client, cmd, rps, duration, threads, true);         // measured
            System.out.println(CSV_HEADER);
            System.out.printf("%s,%s,%d,%d,%.0f,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d%n", proto, "text", size, rps, s.achieved, s.ok, s.errors,
                    s.corrected.getValueAtPercentile(50), s.corrected.getValueAtPercentile(90),
                    s.corrected.getValueAtPercentile(95), s.corrected.getValueAtPercentile(99),
                    s.corrected.getValueAtPercentile(99.9), s.corrected.getMaxValue(),
                    s.service.getValueAtPercentile(50), s.service.getValueAtPercentile(99));
        }
    }

    record Stats(Histogram corrected, Histogram service, long ok, long errors, double achieved) {
    }

    static Stats run(IngestClient client, PublishEventCommand cmd, int rps, int seconds, int threads, boolean record)
            throws InterruptedException {
        ConcurrentHistogram corrected = new ConcurrentHistogram(3);
        ConcurrentHistogram service = new ConcurrentHistogram(3);
        AtomicLong ok = new AtomicLong();
        AtomicLong errors = new AtomicLong();
        AtomicLong lastDone = new AtomicLong();
        ExecutorService pool = Executors.newFixedThreadPool(threads);

        long n = (long) rps * seconds;
        long interval = 1_000_000_000L / rps;
        long t0 = System.nanoTime() + 50_000_000L;
        for (long i = 0; i < n; i++) {
            long intended = t0 + i * interval;
            long now;
            while ((now = System.nanoTime()) < intended) {
                long d = intended - now;
                if (d > 200_000) LockSupport.parkNanos(d - 100_000); else Thread.onSpinWait();
            }
            pool.execute(() -> {
                long start = System.nanoTime();
                boolean good = true;
                try {
                    // Realistic SIS #1 contract: the producer puts a fresh event_id in the BODY,
                    // no override header. The 36-char UUID becomes part of the on-wire payload for
                    // both REST (in the JSON) and gRPC (in the protobuf), so size comparisons are fair.
                    PublishEventCommand c = new PublishEventCommand(
                            UUID.randomUUID().toString(), cmd.userId(), cmd.type(), cmd.payload());
                    client.publishEvent(c, null);
                } catch (Exception e) {
                    good = false;
                }
                long end = System.nanoTime();
                lastDone.accumulateAndGet(end, Math::max);
                if (record) {
                    if (good) ok.incrementAndGet(); else errors.incrementAndGet();
                    corrected.recordValue(Math.max(1, (end - intended) / 1000));
                    service.recordValue(Math.max(1, (end - start) / 1000));
                }
            });
        }
        pool.shutdown();
        pool.awaitTermination(120, TimeUnit.SECONDS);
        double achieved = n / Math.max(1e-9, (lastDone.get() - t0) / 1e9);
        return new Stats(corrected, service, ok.get(), errors.get(), Math.min(achieved, rps * 1.0));
    }
}
