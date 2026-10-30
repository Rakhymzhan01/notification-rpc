package sis2.resilience;

/**
 * Retry budget (token bucket): every first attempt adds {@code ratio} tokens, every retry costs one.
 * With ratio = 0.1 retries can never exceed ~10% of traffic, so a mass failure cannot be amplified into a retry storm.
 */
public final class RetryBudget {
    private final double ratio;
    private final double maxTokens;
    private double tokens;

    public RetryBudget(double ratio, double maxTokens) {
        this.ratio = ratio;
        this.maxTokens = maxTokens;
        this.tokens = maxTokens; // cold start: allow a few retries
    }

    public synchronized void onRequest() {
        tokens = Math.min(maxTokens, tokens + ratio);
    }

    public synchronized boolean tryAcquireRetry() {
        if (tokens >= 1.0) {
            tokens -= 1.0;
            return true;
        }
        return false;
    }
}
