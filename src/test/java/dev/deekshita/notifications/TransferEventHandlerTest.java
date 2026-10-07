package dev.deekshita.notifications;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TransferEventHandlerTest {

    private final ProcessedEventRepository repository = mock(ProcessedEventRepository.class);
    private final NotificationGateway gateway = mock(NotificationGateway.class);
    private final TransferEventHandler handler =
            new TransferEventHandler(new ObjectMapper().registerModule(new JavaTimeModule()), repository, gateway);

    @Test
    void missingFieldsAreRejectedAsInvalid() {
        assertThatThrownBy(() -> handler.handle("{\"eventId\":\"" + UUID.randomUUID() + "\"}"))
                .isInstanceOf(InvalidEventException.class);
        verify(gateway, never()).notifyTransferCompleted(any());
    }

    @Test
    void alreadyProcessedEventIsSkipped() {
        UUID eventId = UUID.randomUUID();
        when(repository.existsById(eventId)).thenReturn(true);
        String payload = """
                {"eventId":"%s","transferId":"%s","fromAccountId":"%s","toAccountId":"%s",
                 "amount":5,"currency":"USD","occurredAt":"2026-01-01T00:00:00Z","extraField":"ignored"}
                """.formatted(eventId, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());

        assertThat(handler.handle(payload)).isFalse();
        verify(gateway, never()).notifyTransferCompleted(any());
    }
}
