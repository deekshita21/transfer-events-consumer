package dev.deekshita.notifications;

/** Port to whatever actually notifies customers (email, SMS, push). */
public interface NotificationGateway {

    void notifyTransferCompleted(TransferCompletedEvent event);
}
