package dev.deekshita.notifications;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Event contract published by payment-transaction-service. Unknown fields are ignored for forward compatibility. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TransferCompletedEvent(
        UUID eventId,
        UUID transferId,
        UUID fromAccountId,
        UUID toAccountId,
        BigDecimal amount,
        String currency,
        Instant occurredAt) {

    boolean isValid() {
        return eventId != null && transferId != null && fromAccountId != null && toAccountId != null
                && amount != null && amount.signum() > 0 && currency != null && currency.length() == 3;
    }
}
