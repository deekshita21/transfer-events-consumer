package dev.deekshita.notifications;

import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

/**
 * Non-blocking retries: a failed message moves to retry topics with exponential backoff
 * instead of blocking the partition. Invalid messages skip retries and go straight to the
 * dead-letter topic, where they are stored for review.
 */
@Component
public class TransferEventListener {

    private static final Logger log = LoggerFactory.getLogger(TransferEventListener.class);

    private final TransferEventHandler handler;
    private final DeadLetterRepository deadLetters;

    public TransferEventListener(TransferEventHandler handler, DeadLetterRepository deadLetters) {
        this.handler = handler;
        this.deadLetters = deadLetters;
    }

    @RetryableTopic(
            attempts = "${app.retry.attempts:4}",
            backoff = @Backoff(delayExpression = "${app.retry.initial-delay-ms:1000}",
                    multiplierExpression = "${app.retry.multiplier:2.0}",
                    maxDelayExpression = "${app.retry.max-delay-ms:30000}"),
            exclude = InvalidEventException.class,
            traversingCauses = "true",
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            kafkaTemplate = "kafkaTemplate")
    @KafkaListener(topics = "${app.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(@Payload String payload) {
        handler.handle(payload);
    }

    @DltHandler
    public void onDeadLetter(@Payload String payload,
                             @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                             @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key,
                             @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) Object reasonHeader) {
        String reason = headerAsString(reasonHeader);
        log.error("Dead-lettered message from topic={} key={} reason={}", topic, key, reason);
        deadLetters.save(new DeadLetter(topic, key, payload, reason));
    }

    private static String headerAsString(Object value) {
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return value == null ? null : value.toString();
    }
}
