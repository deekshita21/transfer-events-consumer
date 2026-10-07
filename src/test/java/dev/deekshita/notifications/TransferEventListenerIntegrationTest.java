package dev.deekshita.notifications;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = "payments.transfer-events")
class TransferEventListenerIntegrationTest {

    private static final String TOPIC = "payments.transfer-events";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Autowired
    private KafkaTemplate<String, String> kafka;

    @Autowired
    private ProcessedEventRepository processedEvents;

    @Autowired
    private DeadLetterRepository deadLetters;

    @MockitoBean
    private NotificationGateway gateway;

    @BeforeEach
    void resetGateway() {
        reset(gateway);
    }

    private static String event(UUID eventId, UUID transferId) {
        return """
                {"eventId":"%s","transferId":"%s",
                 "fromAccountId":"%s","toAccountId":"%s",
                 "amount":25.00,"currency":"USD","occurredAt":"2026-01-15T10:15:30Z"}
                """.formatted(eventId, transferId, UUID.randomUUID(), UUID.randomUUID());
    }

    @Test
    void validEventIsProcessedOnce() {
        UUID eventId = UUID.randomUUID();
        kafka.send(TOPIC, "t1", event(eventId, UUID.randomUUID()));

        await().atMost(TIMEOUT).until(() -> processedEvents.existsById(eventId));
        verify(gateway, times(1)).notifyTransferCompleted(any());
    }

    @Test
    void duplicateDeliveryDoesNotNotifyTwice() {
        UUID eventId = UUID.randomUUID();
        UUID transferId = UUID.randomUUID();
        String payload = event(eventId, transferId);

        kafka.send(TOPIC, transferId.toString(), payload);
        await().atMost(TIMEOUT).until(() -> processedEvents.existsById(eventId));
        kafka.send(TOPIC, transferId.toString(), payload);

        // give the second delivery time to be consumed, then confirm it was skipped
        await().pollDelay(Duration.ofSeconds(2)).atMost(TIMEOUT).untilAsserted(() ->
                verify(gateway, times(1)).notifyTransferCompleted(any()));
    }

    @Test
    void transientFailureIsRetriedUntilItSucceeds() {
        doThrow(new TransientNotificationException("provider timeout"))
                .doNothing()
                .when(gateway).notifyTransferCompleted(any());
        UUID eventId = UUID.randomUUID();

        kafka.send(TOPIC, "t2", event(eventId, UUID.randomUUID()));

        await().atMost(TIMEOUT).until(() -> processedEvents.existsById(eventId));
        verify(gateway, times(2)).notifyTransferCompleted(any());
    }

    @Test
    void invalidPayloadGoesStraightToDeadLetterWithoutRetries() {
        long before = deadLetters.count();

        kafka.send(TOPIC, "bad", "{not json");

        await().atMost(TIMEOUT).until(() -> deadLetters.count() == before + 1);
        verify(gateway, never()).notifyTransferCompleted(any());
        assertThat(deadLetters.findAll()).anyMatch(d -> "{not json".equals(d.getPayload()));
    }

    @Test
    void persistentFailureEndsInDeadLetterAfterRetries() {
        doThrow(new TransientNotificationException("provider down"))
                .when(gateway).notifyTransferCompleted(any());
        UUID eventId = UUID.randomUUID();
        String payload = event(eventId, UUID.randomUUID());
        long before = deadLetters.count();

        kafka.send(TOPIC, "t3", payload);

        await().atMost(TIMEOUT).until(() -> deadLetters.count() == before + 1);
        assertThat(processedEvents.existsById(eventId)).isFalse();
        verify(gateway, times(3)).notifyTransferCompleted(any());
        doNothing().when(gateway).notifyTransferCompleted(any());
    }
}
