package dev.deekshita.notifications;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Business handling for one event. The processed-event row is written only after the
 * notification succeeds, in the same transaction, so a failure leaves nothing behind
 * and the retry starts clean.
 */
@Service
public class TransferEventHandler {

    private static final Logger log = LoggerFactory.getLogger(TransferEventHandler.class);

    private final ObjectMapper objectMapper;
    private final ProcessedEventRepository processedEvents;
    private final NotificationGateway notificationGateway;

    public TransferEventHandler(ObjectMapper objectMapper, ProcessedEventRepository processedEvents,
                                NotificationGateway notificationGateway) {
        this.objectMapper = objectMapper;
        this.processedEvents = processedEvents;
        this.notificationGateway = notificationGateway;
    }

    /** @return true if the event was handled now, false if it was a duplicate */
    @Transactional
    public boolean handle(String payload) {
        TransferCompletedEvent event = parse(payload);
        if (processedEvents.existsById(event.eventId())) {
            log.info("Skipping duplicate eventId={}", event.eventId());
            return false;
        }
        notificationGateway.notifyTransferCompleted(event);
        processedEvents.save(new ProcessedEvent(event.eventId()));
        log.info("Processed eventId={} transferId={}", event.eventId(), event.transferId());
        return true;
    }

    private TransferCompletedEvent parse(String payload) {
        try {
            TransferCompletedEvent event = objectMapper.readValue(payload, TransferCompletedEvent.class);
            if (event == null || !event.isValid()) {
                throw new InvalidEventException("Event is missing required fields", null);
            }
            return event;
        } catch (JsonProcessingException e) {
            throw new InvalidEventException("Payload is not valid JSON for TransferCompletedEvent", e);
        }
    }
}
