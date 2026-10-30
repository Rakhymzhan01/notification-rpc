package sis2.bench;

import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;
import sis2.core.ClientOptions;
import sis2.core.FaultInjector;
import sis2.core.InMemoryIdempotencyStore;
import sis2.core.IngestClient;
import sis2.core.IngestCore;
import sis2.core.PublishEventCommand;
import sis2.grpc.GrpcIngestClient;
import sis2.grpc.GrpcIngestServer;
import sis2.rest.RestIngestClient;
import sis2.rest.RestIngestServer;

/**
 * On-wire payload size: REST/JSON vs gRPC/protobuf, with and without gzip.
 * Only "one event per call" shapes - the fan-out suite was removed because the SIS #1 Ingest API
 * accepts exactly one event per request. event_id goes in the BODY (fresh UUID per call), not in a
 * header, so the 36-char UUID is counted into the payload for both protocols.
 * <ul>
 *   <li>first   = everything the very first request costs (connection setup, HTTP/2 preface + SETTINGS, full headers);</li>
 *   <li>steady  = average per request after the connection is warm (HPACK table filled, keep-alive in use).</li>
 * </ul>
 */
public final class PayloadBench {
    static final int STEADY_REQUESTS = 50;
    /** Fixed 36-char representative UUID used ONLY for the body_encoded_bytes column (steady state is random). */
    static final String REPRESENTATIVE_EVENT_ID = "00000000-0000-0000-0000-000000000000";

    public static void main(String[] args) throws Exception {
        IngestCore core = new IngestCore(new InMemoryIdempotencyStore(Duration.ofMinutes(10)), new FaultInjector());
        try (GrpcIngestServer grpc = new GrpcIngestServer(core, 0, 8).start();
             RestIngestServer rest = new RestIngestServer(core, 0, 8).start()) {

            System.out.println("proto,mode,size,body_encoded_bytes,first_c2s,first_s2c,steady_c2s,steady_s2c");
            // SIS #1 sizes: 100 B (small), 1 KB (average), 10 KB (max). 100 KB kept as a stress reference.
            List<Integer> sizes = List.of(100, 1_000, 10_000, 100_000);
            for (int size : sizes) {
                PublishEventCommand template = Payloads.text(size);
                for (String proto : List.of("rest", "rest-gzip", "grpc", "grpc-gzip")) {
                    int target = proto.startsWith("grpc") ? grpc.port() : rest.port();
                    measure(proto, size, template, target);
                }
            }
        }
        System.exit(0);
    }

    static void measure(String proto, int size, PublishEventCommand template, int targetPort) throws Exception {
        try (ByteCountingProxy proxy = new ByteCountingProxy(targetPort)) {
            ClientOptions opts = ClientOptions.benchmark(proto.endsWith("-gzip"));
            try (IngestClient c = proto.startsWith("grpc")
                    ? new GrpcIngestClient("127.0.0.1", proxy.port(), opts)
                    : new RestIngestClient("127.0.0.1", proxy.port(), opts)) {

                c.publishEvent(withFreshId(template), null);
                Thread.sleep(300);                               // let trailing frames (acks, window updates) settle
                long f1 = proxy.clientToServer.get(), f2 = proxy.serverToClient.get();

                for (int i = 0; i < STEADY_REQUESTS; i++) c.publishEvent(withFreshId(template), null);
                Thread.sleep(300);
                long s1 = (proxy.clientToServer.get() - f1) / STEADY_REQUESTS;
                long s2 = (proxy.serverToClient.get() - f2) / STEADY_REQUESTS;

                System.out.printf("%s,%s,%d,%d,%d,%d,%d,%d%n", proto, "text", size, bodyBytes(proto, template), f1, f2, s1, s2);
            }
        }
    }

    /** Fresh event_id in the body per call - the realistic SIS #1 contract (producer generates event_id). */
    static PublishEventCommand withFreshId(PublishEventCommand t) {
        return new PublishEventCommand(UUID.randomUUID().toString(), t.userId(), t.type(), t.payload());
    }

    /**
     * Size of the encoded request body only (no headers/framing). A representative 36-char UUID is used
     * for event_id so the number reflects what the LoadGenerator actually sends on the wire.
     */
    static long bodyBytes(String proto, PublishEventCommand template) throws Exception {
        PublishEventCommand cmd = new PublishEventCommand(
                REPRESENTATIVE_EVENT_ID, template.userId(), template.type(), template.payload());
        byte[] raw = proto.startsWith("grpc")
                ? sis2.proto.PublishEventRequest.newBuilder()
                        .setEventId(cmd.eventId())
                        .setUserId(cmd.userId() == null ? "" : cmd.userId())
                        .setType(cmd.type() == null ? "" : cmd.type())
                        .setPayload(cmd.payload() == null ? "" : cmd.payload())
                        .build().toByteArray()
                : RestIngestServer.JSON.writeValueAsBytes(
                        new RestIngestServer.EventBody(cmd.eventId(), cmd.userId(), cmd.type(), cmd.payload()));
        if (!proto.endsWith("-gzip")) return raw.length;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (GZIPOutputStream g = new GZIPOutputStream(bos)) {
            g.write(raw);
        }
        return bos.size();
    }
}
