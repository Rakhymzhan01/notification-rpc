package sis2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import sis2.core.ClientOptions;
import sis2.core.IngestClient;
import sis2.core.IngestCore;
import sis2.grpc.GrpcIngestClient;
import sis2.grpc.GrpcIngestServer;
import sis2.resilience.CircuitBreaker;
import sis2.resilience.RetryPolicy;
import sis2.resilience.RetryPolicy.Config;
import sis2.resilience.RetryPolicy.Jitter;

class GrpcContractTest extends AbstractProtocolTest {
    private int port;
    private GrpcIngestClient last;

    @Override
    protected AutoCloseable startServer(IngestCore core) throws Exception {
        GrpcIngestServer s = new GrpcIngestServer(core, 0, 8).start();
        port = s.port();
        return s;
    }

    @Override
    protected IngestClient newClient(ClientOptions opts) {
        GrpcIngestClient c = new GrpcIngestClient("127.0.0.1", port, opts);
        if (last == null || opts.gzip() == false) last = c;
        return c;
    }

    @Override
    protected RetryPolicy policy() {
        return last.retryPolicy();
    }

    @Test
    void deadlineIsPropagated_serverDoesNotCommitWorkNobodyWaitsFor() throws Exception {
        faults.delayMillis(400);                                    // server needs 400 ms
        ClientOptions oneShot = fast().withPerAttemptTimeout(Duration.ofMillis(100))
                .withRetry(Config.noRetry(Duration.ofSeconds(2)));
        try (GrpcIngestClient c = new GrpcIngestClient("127.0.0.1", port, oneShot)) {
            assertThrows(io.grpc.StatusRuntimeException.class, () -> c.publishEvent(cmd(), UUID.randomUUID().toString()));
        }
        Thread.sleep(600);
        assertEquals(0, core.kafkaWrites(), "gRPC cancels the server Context at the deadline; handler skipped the Kafka write");
    }

    @Test
    void circuitBreakerFailsFastWhenServerIsDown() throws Exception {
        faults.failBeforeRate(1.0);
        var cb = new CircuitBreaker(3, Duration.ofSeconds(30), 1);
        ClientOptions o = fast().withBreaker(cb)
                .withRetry(new Config(10, Duration.ofMillis(1), Duration.ofMillis(2), 1.0, Jitter.NONE, Duration.ofSeconds(5)));
        try (GrpcIngestClient c = new GrpcIngestClient("127.0.0.1", port, o)) {
            assertThrows(Exception.class, () -> c.publishEvent(cmd(), UUID.randomUUID().toString()));
            // 3 real attempts open the circuit; the 4th attempt fails fast (CircuitOpenException, not retryable)
            assertEquals(4, c.retryPolicy().attempts.sum());
            assertEquals(CircuitBreaker.State.OPEN, cb.state());
        }
    }
}
