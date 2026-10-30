package sis2.rest;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import sis2.core.ClientOptions;
import sis2.core.IngestClient;
import sis2.core.PublishEventCommand;
import sis2.core.Result;
import sis2.resilience.CircuitBreaker;
import sis2.resilience.RetryPolicy;

/**
 * REST/JSON baseline client (HTTP/1.1, java.net.http.HttpClient) for the Ingest API,
 * with the same timeouts / retry / idempotency behaviour as the gRPC client.
 */
public final class RestIngestClient implements IngestClient {

    /** Non-2xx answer. */
    public static final class HttpStatusException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        public final int status;

        public HttpStatusException(int status, String body) {
            super("HTTP " + status + ": " + body);
            this.status = status;
        }
    }

    private static final Set<Integer> RETRYABLE_STATUS = Set.of(429, 502, 503, 504);

    private final HttpClient http;
    private final URI uri;
    private final ClientOptions opts;
    private final RetryPolicy retry;

    public RestIngestClient(String host, int port, ClientOptions opts) {
        this.opts = opts;
        this.uri = URI.create("http://" + host + ":" + port + RestIngestServer.PATH);
        this.http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)         // classic REST baseline
                .connectTimeout(opts.connectTimeout())         // connect timeout
                .build();
        this.retry = new RetryPolicy(opts.retry(), opts.budget());
    }

    public RetryPolicy retryPolicy() {
        return retry;
    }

    @Override
    public Result publishEvent(PublishEventCommand cmd, String overrideKey) throws Exception {
        byte[] json = RestIngestServer.JSON.writeValueAsBytes(
                new RestIngestServer.EventBody(cmd.eventId(), cmd.userId(), cmd.type(), cmd.payload()));
        byte[] payload = opts.gzip() ? gzip(json) : json;
        CircuitBreaker breaker = opts.breaker();

        return retry.execute((attempt, remaining) -> {
            Duration timeout = remaining.compareTo(opts.perAttemptTimeout()) < 0 ? remaining : opts.perAttemptTimeout();
            HttpRequest.Builder rb = HttpRequest.newBuilder(uri)
                    .timeout(timeout)                      // per-attempt (request) timeout
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(payload));
            if (overrideKey != null) rb.header(RestIngestServer.IDEMPOTENCY_HEADER, overrideKey);
            if (opts.gzip()) rb.header("Content-Encoding", "gzip").header("Accept-Encoding", "gzip");
            HttpRequest req = rb.build();
            java.util.concurrent.Callable<Result> call = () -> doRequest(req);
            return breaker == null ? call.call() : breaker.call(call, RestIngestClient::isRetryable);
        }, RestIngestClient::isRetryable);
    }

    private Result doRequest(HttpRequest req) throws IOException, InterruptedException {
        HttpResponse<byte[]> resp = http.send(req, HttpResponse.BodyHandlers.ofByteArray());
        byte[] body = resp.body();
        if ("gzip".equalsIgnoreCase(resp.headers().firstValue("Content-Encoding").orElse(""))) {
            body = new GZIPInputStream(new ByteArrayInputStream(body)).readAllBytes();
        }
        // Any 2xx is success; 202 Accepted is the normal path for POST /v1/events.
        if (resp.statusCode() / 100 != 2) {
            throw new HttpStatusException(resp.statusCode(), new String(body));
        }
        JsonNode n = RestIngestServer.JSON.readTree(body);
        return new Result(n.get("eventId").asText(), n.get("accepted").asBoolean(), n.get("replayed").asBoolean());
    }

    /** Network errors and timeouts (IOException) and 429/502/503/504 are transient. Other 4xx are not. */
    static boolean isRetryable(Throwable t) {
        if (t instanceof HttpStatusException h) return RETRYABLE_STATUS.contains(h.status);
        return t instanceof IOException; // ConnectException, HttpTimeoutException, connection reset ...
    }

    private static byte[] gzip(byte[] in) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(in.length / 2 + 32);
        try (GZIPOutputStream g = new GZIPOutputStream(bos)) {
            g.write(in);
        }
        return bos.toByteArray();
    }

    @Override
    public void close() {
        // java.net.http.HttpClient has no close() before JDK 21; connections idle out on their own
    }
}
