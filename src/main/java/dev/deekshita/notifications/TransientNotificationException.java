package dev.deekshita.notifications;

/** A downstream dependency failed in a way that may succeed on retry (timeout, 503, throttling). */
public class TransientNotificationException extends RuntimeException {

    public TransientNotificationException(String message) {
        super(message);
    }
}
