package sis2.app;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import sis2.core.ClientOptions;
import sis2.core.FaultInjector;
import sis2.core.InMemoryIdempotencyStore;
import sis2.core.IngestClient;
import sis2.core.IngestCore;
import sis2.core.PublishEventCommand;
import sis2.grpc.GrpcIngestClient;
import sis2.grpc.GrpcIngestServer;
import sis2.resilience.CircuitBreaker;
import sis2.resilience.RetryPolicy;
import sis2.rest.RestIngestClient;
import sis2.rest.RestIngestServer;

/**
 * Live demo for the defence: java -cp ... sis2.app.Demo
 * Shows idempotency, retries with backoff, non-retryable errors, deadline propagation and the circuit breaker
 * on the real gRPC server and on the REST baseline. Side effect = "publish to Kafka" (simulated).
 */
public final class Demo {
    static final PublishEventCommand CMD = new PublishEventCommand(null, "user-42", "order.created", "demo payload");

    public static void main(String[] args) throws Exception {
        FaultInjector faults = new FaultInjector();
        IngestCore core = new IngestCore(new InMemoryIdempotencyStore(Duration.ofHours(24)), faults);
        try (GrpcIngestServer grpc = new GrpcIngestServer(core, 0, 8).start();
             RestIngestServer rest = new RestIngestServer(core, 0, 8).start()) {

            ClientOptions opts = ClientOptions.defaults();
            for (String proto : List.of("grpc", "rest")) {
                System.out.println("\n================ " + proto.toUpperCase() + " ================");
                GrpcIngestClient gc = null;
                RestIngestClient rc = null;
                IngestClient c;
                RetryPolicy policy;
                if (proto.equals("grpc")) { gc = new GrpcIngestClient("127.0.0.1", grpc.port(), opts); c = gc; policy = gc.retryPolicy(); }
                else { rc = new RestIngestClient("127.0.0.1", rest.port(), opts); c = rc; policy = rc.retryPolicy(); }

                // 1. response lost: with and without idempotency key (= event_id in the body)
                long before = core.kafkaWrites();
                faults.failAfterNext(1);
                c.publishEvent(CMD, null);
                System.out.printf("1a response lost, NO event_id : Kafka writes +%d  (duplicate!)%n", core.kafkaWrites() - before);
                before = core.kafkaWrites();
                faults.failAfterNext(1);
                var r = c.publishEvent(CMD, UUID.randomUUID().toString());
                System.out.printf("1b response lost, WITH key    : Kafka writes +%d  (replayed=%s)%n", core.kafkaWrites() - before, r.replayed());

                // 2. request lost twice: retries with exponential backoff + full jitter
                long a0 = policy.attempts.sum();
                long t0 = System.nanoTime();
                faults.failBeforeNext(2);
                c.publishEvent(CMD, UUID.randomUUID().toString());
                System.out.printf("2  two transient failures     : %d attempts, %d ms total (backoff+jitter)%n",
                        policy.attempts.sum() - a0, (System.nanoTime() - t0) / 1_000_000);

                // 3. validation error must not be retried
                a0 = policy.attempts.sum();
                try {
                    c.publishEvent(new PublishEventCommand(null, "", "order.created", "x"), UUID.randomUUID().toString());
                } catch (Exception e) {
                    System.out.printf("3  invalid request            : %d attempt(s), error = %s%n", policy.attempts.sum() - a0, e.getMessage());
                }

                // 4. client deadline shorter than server work, no retries
                ClientOptions oneShot = opts.withPerAttemptTimeout(Duration.ofMillis(100)).withRetry(RetryPolicy.Config.noRetry(Duration.ofSeconds(2)));
                before = core.kafkaWrites();
                faults.delayNext(1, 400);
                try (IngestClient fast = proto.equals("grpc")
                        ? new GrpcIngestClient("127.0.0.1", grpc.port(), oneShot)
                        : new RestIngestClient("127.0.0.1", rest.port(), oneShot)) {
                    fast.publishEvent(CMD, UUID.randomUUID().toString());
                } catch (Exception e) {
                    // expected: deadline exceeded / timeout
                }
                Thread.sleep(600);
                System.out.printf("4  client gave up at 100ms, server needs 400ms: Kafka writes +%d (%s)%n", core.kafkaWrites() - before,
                        proto.equals("grpc") ? "gRPC cancelled the work" : "no deadline propagation over plain HTTP");

                // 5. circuit breaker against a dead server
                faults.failBeforeRate(1.0);
                CircuitBreaker cb = new CircuitBreaker(3, Duration.ofSeconds(5), 1);
                ClientOptions withCb = opts.withBreaker(cb);
                try (IngestClient cbClient = proto.equals("grpc")
                        ? new GrpcIngestClient("127.0.0.1", grpc.port(), withCb)
                        : new RestIngestClient("127.0.0.1", rest.port(), withCb)) {
                    for (int i = 0; i < 3; i++) {
                        long s = System.nanoTime();
                        try { cbClient.publishEvent(CMD, UUID.randomUUID().toString()); } catch (Exception e) { /* expected */ }
                        System.out.printf("5.%d dead server, breaker=%s, call took %d ms%n", i + 1, cb.state(), (System.nanoTime() - s) / 1_000_000);
                    }
                }
                faults.failBeforeRate(0.0);
                c.close();
            }
            System.out.println("\nTotal simulated Kafka writes: " + core.kafkaWrites());
        }
        System.exit(0);
    }
}
