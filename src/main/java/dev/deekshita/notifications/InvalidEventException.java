package dev.deekshita.notifications;

/** The message can never be processed (bad JSON or missing fields); retrying will not help. */
public class InvalidEventException extends RuntimeException {

    public InvalidEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
