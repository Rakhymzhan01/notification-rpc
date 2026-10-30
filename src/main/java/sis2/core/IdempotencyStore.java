package sis2.core;

import java.util.function.Supplier;

/**
 * Idempotency store contract. The in-memory implementation is enough for the assignment;
 * in production this would be Redis (SET key NX EX ttl) or a unique-index table.
 */
public interface IdempotencyStore {

    /**
     * Runs {@code action} at most once per live key.
     * <ul>
     *   <li>first call for a key: executes the action and remembers the result;</li>
     *   <li>repeat (also concurrent) call with the same fingerprint: returns the remembered result with replayed=true;</li>
     *   <li>same key, different fingerprint: throws {@link ServiceException.Kind#KEY_REUSED};</li>
     *   <li>if the action throws, nothing is remembered, so a retry can execute it.</li>
     * </ul>
     */
    Result execute(String key, String fingerprint, Supplier<Result> action);

    int size();
}
