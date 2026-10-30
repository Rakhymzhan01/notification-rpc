package sis2.app;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import sis2.core.FaultInjector;
import sis2.core.InMemoryIdempotencyStore;
import sis2.core.IngestCore;
import sis2.grpc.GrpcIngestServer;
import sis2.rest.RestIngestServer;

/**
 * java -cp ... sis2.app.ServerMain --proto grpc|rest --port 50051 [--threads 16] [--delay-ms 0]
 *        [--fail-before 0.0] [--fail-after 0.0] [--ttl-sec 86400]
 *
 * Starts the Ingest API (SIS #1) on the chosen transport.
 */
public final class ServerMain {
    public static void main(String[] args) throws Exception {
        Map<String, String> a = parse(args);
        String proto = a.getOrDefault("proto", "grpc");
        int port = Integer.parseInt(a.getOrDefault("port", "50051"));
        int threads = Integer.parseInt(a.getOrDefault("threads", "16"));

        FaultInjector faults = new FaultInjector()
                .delayMillis(Long.parseLong(a.getOrDefault("delay-ms", "0")))
                .failBeforeRate(Double.parseDouble(a.getOrDefault("fail-before", "0")))
                .failAfterRate(Double.parseDouble(a.getOrDefault("fail-after", "0")));
        InMemoryIdempotencyStore store = new InMemoryIdempotencyStore(
                Duration.ofSeconds(Long.parseLong(a.getOrDefault("ttl-sec", "86400")))).startSweeper(Duration.ofSeconds(30));
        IngestCore core = new IngestCore(store, faults);

        AutoCloseable server = switch (proto) {
            case "grpc" -> new GrpcIngestServer(core, port, threads).start();
            case "rest" -> new RestIngestServer(core, port, threads).start();
            default -> throw new IllegalArgumentException("--proto grpc|rest");
        };
        System.out.println("READY " + proto + " port=" + port + " threads=" + threads);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.close();
            } catch (Exception ignored) {
            }
        }));
        new CountDownLatch(1).await();
    }

    static Map<String, String> parse(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) m.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        return m;
    }
}
