package dev.deekshita.notifications;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Demo adapter: logs instead of calling a real provider. Logs ids only, never personal data. */
@Component
public class LoggingNotificationGateway implements NotificationGateway {

    private static final Logger log = LoggerFactory.getLogger(LoggingNotificationGateway.class);

    @Override
    public void notifyTransferCompleted(TransferCompletedEvent event) {
        log.info("Notification sent for transfer={} amount={} {}", event.transferId(), event.amount(), event.currency());
    }
}
