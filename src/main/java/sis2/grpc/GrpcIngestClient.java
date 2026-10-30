package sis2.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import java.util.concurrent.TimeUnit;
import sis2.core.ClientOptions;
import sis2.core.IngestClient;
import sis2.core.PublishEventCommand;
import sis2.core.Result;
import sis2.proto.NotificationServiceGrpc;
import sis2.proto.PublishEventResponse;
import sis2.resilience.CircuitBreaker;
import sis2.resilience.RetryPolicy;

/**
 * gRPC client for the Ingest API: per-attempt deadline, retry with backoff+jitter on transient codes,
 * optional explicit idempotency key in metadata (default: event_id from the body).
 * One ManagedChannel = one HTTP/2 connection that multiplexes all concurrent calls.
 */
public final class GrpcIngestClient implements IngestClient {

    private final ManagedChannel channel;
    private final NotificationServiceGrpc.NotificationServiceBlockingStub stub;
    private final ClientOptions opts;
    private final RetryPolicy retry;

    public GrpcIngestClient(String host, int port, ClientOptions opts) {
        this.opts = opts;
        this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        this.stub = NotificationServiceGrpc.newBlockingStub(channel);
        this.retry = new RetryPolicy(opts.retry(), opts.budget());
    }

    public RetryPolicy retryPolicy() {
        return retry;
    }

    @Override
    public Result publishEvent(PublishEventCommand cmd, String overrideKey) throws Exception {
        sis2.proto.PublishEventRequest req = Converters.toProto(cmd);
        Metadata md = new Metadata();
        if (overrideKey != null) md.put(GrpcIngestServer.IDEMPOTENCY_KEY, overrideKey);
        io.grpc.ClientInterceptor attach = MetadataUtils.newAttachHeadersInterceptor(md);
        CircuitBreaker breaker = opts.breaker();

        PublishEventResponse resp = retry.execute((attempt, remaining) -> {
            // deadline of this attempt = min(per-attempt timeout, what is left of the overall timeout)
            long ms = Math.max(1, Math.min(opts.perAttemptTimeout().toMillis(), remaining.toMillis()));
            var base = stub.withInterceptors(attach).withDeadlineAfter(ms, TimeUnit.MILLISECONDS);
            var s = opts.gzip() ? base.withCompression("gzip") : base;
            java.util.concurrent.Callable<PublishEventResponse> call = () -> s.publishEvent(req);
            return breaker == null ? call.call() : breaker.call(call, GrpcIngestClient::isServerSideFailure);
        }, GrpcIngestClient::isRetryable);

        return new Result(resp.getEventId(), resp.getAccepted(), resp.getReplayed());
    }

    /** Transient codes only. INVALID_ARGUMENT, FAILED_PRECONDITION, NOT_FOUND, PERMISSION_DENIED... never retried. */
    static boolean isRetryable(Throwable t) {
        if (!(t instanceof StatusRuntimeException e)) return false; // includes CircuitOpenException
        Status.Code c = e.getStatus().getCode();
        return c == Status.Code.UNAVAILABLE || c == Status.Code.DEADLINE_EXCEEDED || c == Status.Code.RESOURCE_EXHAUSTED;
    }

    /** What the circuit breaker counts as "the downstream is sick". */
    static boolean isServerSideFailure(Throwable t) {
        return isRetryable(t);
    }

    @Override
    public void close() {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(2, TimeUnit.SECONDS)) channel.shutdownNow();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
        }
    }
}
