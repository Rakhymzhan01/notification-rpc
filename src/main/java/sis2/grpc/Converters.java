package sis2.grpc;

import sis2.core.PublishEventCommand;
import sis2.proto.PublishEventRequest;

final class Converters {
    private Converters() {
    }

    static PublishEventCommand toCommand(PublishEventRequest r) {
        return new PublishEventCommand(
                emptyToNull(r.getEventId()),
                r.getUserId(),
                r.getType(),
                r.getPayload());
    }

    static PublishEventRequest toProto(PublishEventCommand c) {
        PublishEventRequest.Builder b = PublishEventRequest.newBuilder()
                .setUserId(c.userId() == null ? "" : c.userId())
                .setType(c.type() == null ? "" : c.type())
                .setPayload(c.payload() == null ? "" : c.payload());
        if (c.eventId() != null) b.setEventId(c.eventId());
        return b.build();
    }

    private static String emptyToNull(String s) {
        return (s == null || s.isEmpty()) ? null : s;
    }
}
