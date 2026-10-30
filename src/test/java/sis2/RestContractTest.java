package sis2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import sis2.core.ClientOptions;
import sis2.core.IngestClient;
import sis2.core.IngestCore;
import sis2.resilience.RetryPolicy;
import sis2.resilience.RetryPolicy.Config;
import sis2.rest.RestIngestClient;
import sis2.rest.RestIngestServer;

class RestContractTest extends AbstractProtocolTest {
    private int port;
    private RestIngestClient last;

    @Override
    protected AutoCloseable startServer(IngestCore core) throws Exception {
        RestIngestServer s = new RestIngestServer(core, 0, 8).start();
        port = s.port();
        return s;
    }

    @Override
    protected IngestClient newClient(ClientOptions opts) {
        RestIngestClient c = new RestIngestClient("127.0.0.1", port, opts);
        if (last == null || opts.gzip() == false) last = c;
        return c;
    }

    @Override
    protected RetryPolicy policy() {
        return last.retryPolicy();
    }

    @Test
    void noDeadlinePropagationOverPlainHttp_abandonedRequestStillCommits() throws Exception {
        // Contrast with gRPC: the server cannot know that the client timed out, so the Kafka write happens anyway.
        faults.delayMillis(400);
        ClientOptions oneShot = fast().withPerAttemptTimeout(Duration.ofMillis(100))
                .withRetry(Config.noRetry(Duration.ofSeconds(2)));
        try (RestIngestClient c = new RestIngestClient("127.0.0.1", port, oneShot)) {
            assertThrows(Exception.class, () -> c.publishEvent(cmd(), UUID.randomUUID().toString()));
        }
        Thread.sleep(700);
        assertEquals(1, core.kafkaWrites());
    }

    /**
     * SIS #1 contract check: POST /v1/events returns HTTP 202 Accepted, both on a fresh request
     * and on a replay after a lost response (the latter also carries the Idempotent-Replayed header).
     * Uses a raw java.net.http.HttpClient so the status code is observed directly, not via the typed client.
     */
    @Test
    void http202OnSuccessAndOnReplayAfterLostResponse() throws Exception {
        HttpClient http = HttpClient.newHttpClient();
        URI uri = URI.create("http://127.0.0.1:" + port + "/v1/events");

        // --- Scenario A: fresh happy-path request -> 202, no Idempotent-Replayed header ---
        String idA = UUID.randomUUID().toString();
        HttpRequest reqA = HttpRequest.newBuilder(uri)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"eventId\":\"" + idA + "\",\"userId\":\"user-1\",\"type\":\"order.created\",\"payload\":\"hi\"}"))
                .build();
        HttpResponse<String> fresh = http.send(reqA, HttpResponse.BodyHandlers.ofString());
        assertEquals(202, fresh.statusCode(), "POST /v1/events must return 202 Accepted (SIS #1 contract)");
        assertFalse(fresh.headers().firstValue("Idempotent-Replayed").isPresent());
        assertEquals(1, core.kafkaWrites());

        // --- Scenario B: response lost after the Kafka write, then a retry hits the idempotency store ---
        String idB = UUID.randomUUID().toString();
        HttpRequest reqB = HttpRequest.newBuilder(uri)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"eventId\":\"" + idB + "\",\"userId\":\"user-1\",\"type\":\"order.created\",\"payload\":\"hi\"}"))
                .build();

        long before = core.kafkaWrites();
        faults.failAfterNext(1);                                   // Kafka write succeeds, then "response is lost"
        HttpResponse<String> lost = http.send(reqB, HttpResponse.BodyHandlers.ofString());
        assertEquals(503, lost.statusCode(), "the lost-response path returns 503 UNAVAILABLE to the client");
        assertEquals(before + 1, core.kafkaWrites(), "Kafka write did happen before the loss");

        // Producer retries with the same event_id -> served from the idempotency store
        HttpResponse<String> replay = http.send(reqB, HttpResponse.BodyHandlers.ofString());
        assertEquals(202, replay.statusCode(), "replay after lost response must return 202 Accepted");
        assertEquals("true", replay.headers().firstValue("Idempotent-Replayed").orElse(""),
                "replayed answers carry Idempotent-Replayed: true");
        assertEquals(before + 1, core.kafkaWrites(), "replay must not duplicate the Kafka write");
    }
}
