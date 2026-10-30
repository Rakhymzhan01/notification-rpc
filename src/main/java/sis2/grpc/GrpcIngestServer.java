package sis2.grpc;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import sis2.core.IngestCore;
import sis2.core.Result;
import sis2.core.ServiceException;
import sis2.proto.NotificationServiceGrpc;
import sis2.proto.PublishEventRequest;
import sis2.proto.PublishEventResponse;

/**
 * gRPC server for the Ingest API (SIS #1 step: Producers -> Ingest API -> Kafka).
 * The service in the .proto is kept as NotificationService (the system is Notification Service);
 * this is the Ingest step inside it.
 */
public final class GrpcIngestServer implements AutoCloseable {

    public static final Metadata.Key<String> IDEMPOTENCY_KEY =
            Metadata.Key.of("idempotency-key", Metadata.ASCII_STRING_MARSHALLER);
    private static final Context.Key<String> CTX_IDEMPOTENCY_KEY = Context.key("idempotency-key");

    private final IngestCore core;
    private final ExecutorService pool;
    private final Server server;

    public GrpcIngestServer(IngestCore core, int port, int threads) {
        this.core = core;
        this.pool = Executors.newFixedThreadPool(threads);
        this.server = ServerBuilder.forPort(port)
                .executor(pool)
                .addService(ServerInterceptors.intercept(new Impl(), new IdempotencyKeyInterceptor()))
                .build();
    }

    public GrpcIngestServer start() throws IOException {
        server.start();
        return this;
    }

    public int port() {
        return server.getPort();
    }

    @Override
    public void close() {
        server.shutdown();
        try {
            if (!server.awaitTermination(3, TimeUnit.SECONDS)) server.shutdownNow();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            server.shutdownNow();
        }
        pool.shutdownNow();
    }

    /** Moves the idempotency-key header from gRPC metadata into the call Context for the handler. */
    private static final class IdempotencyKeyInterceptor implements ServerInterceptor {
        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
            Context ctx = Context.current().withValue(CTX_IDEMPOTENCY_KEY, headers.get(IDEMPOTENCY_KEY));
            return Contexts.interceptCall(ctx, call, headers, next);
        }
    }

    private final class Impl extends NotificationServiceGrpc.NotificationServiceImplBase {
        @Override
        public void publishEvent(PublishEventRequest req, StreamObserver<PublishEventResponse> out) {
            ServerCallStreamObserver<PublishEventResponse> so = (ServerCallStreamObserver<PublishEventResponse>) out;
            try {
                // The client's deadline arrives as grpc-timeout; the Context is cancelled when it passes.
                Result r = core.publishEvent(CTX_IDEMPOTENCY_KEY.get(),
                        Converters.toCommand(req), Context.current()::isCancelled);
                if (so.isCancelled()) return;
                out.onNext(PublishEventResponse.newBuilder()
                        .setEventId(r.eventId())
                        .setAccepted(r.accepted())
                        .setReplayed(r.replayed())
                        .build());
                out.onCompleted();
            } catch (ServiceException e) {
                if (!so.isCancelled()) out.onError(toStatus(e).asRuntimeException());
            }
        }
    }

    static Status toStatus(ServiceException e) {
        return switch (e.kind()) {
            case INVALID_ARGUMENT -> Status.INVALID_ARGUMENT.withDescription(e.getMessage());
            case KEY_REUSED -> Status.FAILED_PRECONDITION.withDescription(e.getMessage());
            case UNAVAILABLE -> Status.UNAVAILABLE.withDescription(e.getMessage());
            case CANCELLED -> Status.CANCELLED.withDescription(e.getMessage());
        };
    }
}
