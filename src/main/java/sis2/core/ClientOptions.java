package sis2.core;

import java.time.Duration;
import sis2.resilience.CircuitBreaker;
import sis2.resilience.RetryBudget;
import sis2.resilience.RetryPolicy;

/**
 * Timeouts and retry settings shared by both clients.
 * <ul>
 *   <li>{@code connectTimeout} - TCP (+TLS) handshake;</li>
 *   <li>{@code perAttemptTimeout} - one attempt (gRPC: call deadline, REST: HttpRequest.timeout);</li>
 *   <li>{@code retry.totalTimeout} - the whole call including all retries and backoff pauses.</li>
 * </ul>
 * Rule: connect &lt; perAttempt &lt; total, and total &gt;= maxAttempts-ish * perAttempt only if you really want that many tries.
 */
public record ClientOptions(Duration connectTimeout, Duration perAttemptTimeout, boolean gzip,
                            RetryPolicy.Config retry, RetryBudget budget, CircuitBreaker breaker) {

    public static ClientOptions defaults() {
        return new ClientOptions(Duration.ofSeconds(1), Duration.ofMillis(500), false,
                RetryPolicy.Config.defaults(), null, null);
    }

    /** For benchmarks: one attempt, no retries, so we measure the protocol and not the resilience layer. */
    public static ClientOptions benchmark(boolean gzip) {
        return new ClientOptions(Duration.ofSeconds(2), Duration.ofSeconds(10), gzip,
                RetryPolicy.Config.noRetry(Duration.ofSeconds(10)), null, null);
    }

    public ClientOptions withRetry(RetryPolicy.Config c) {
        return new ClientOptions(connectTimeout, perAttemptTimeout, gzip, c, budget, breaker);
    }

    public ClientOptions withPerAttemptTimeout(Duration d) {
        return new ClientOptions(connectTimeout, d, gzip, retry, budget, breaker);
    }

    public ClientOptions withGzip(boolean g) {
        return new ClientOptions(connectTimeout, perAttemptTimeout, g, retry, budget, breaker);
    }

    public ClientOptions withBudget(RetryBudget b) {
        return new ClientOptions(connectTimeout, perAttemptTimeout, gzip, retry, b, breaker);
    }

    public ClientOptions withBreaker(CircuitBreaker b) {
        return new ClientOptions(connectTimeout, perAttemptTimeout, gzip, retry, budget, b);
    }
}
