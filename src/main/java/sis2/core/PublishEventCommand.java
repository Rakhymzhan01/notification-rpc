package sis2.core;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Transport-independent request. Both the gRPC and the REST server convert to this.
 * eventId may be null/blank; the server fills in a fresh UUID when it is.
 */
public record PublishEventCommand(String eventId, String userId, String type, String payload) {

    public void validate() {
        if (userId == null || userId.isBlank()) {
            throw new ServiceException(ServiceException.Kind.INVALID_ARGUMENT, "user_id must not be blank");
        }
        if (type == null || type.isBlank()) {
            throw new ServiceException(ServiceException.Kind.INVALID_ARGUMENT, "type must not be blank");
        }
        // payload may be empty; SIS #1 says average ~1 KB and max ~10 KB (not enforced here)
    }

    /**
     * Fingerprint of the request CONTENT (not eventId - that is the idempotency key).
     * The store keeps it next to the key so that "same key, different payload" is detected
     * and rejected instead of silently replaying a wrong response.
     */
    public String fingerprint() {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(userId.getBytes(StandardCharsets.UTF_8));
            md.update((byte) 0);
            md.update(type.getBytes(StandardCharsets.UTF_8));
            md.update((byte) 0);
            md.update((payload == null ? "" : payload).getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
