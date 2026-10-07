package dev.deekshita.notifications;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

/** Message that exhausted retries or was invalid, kept for operations review and replay. */
@Entity
@Table(name = "dead_letters")
public class DeadLetter {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "source_topic", nullable = false, length = 200)
    private String sourceTopic;

    @Column(name = "message_key", length = 200)
    private String messageKey;

    @Column(nullable = false, length = 4000)
    private String payload;

    @Column(length = 1000)
    private String reason;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    protected DeadLetter() {
        // for JPA
    }

    public DeadLetter(String sourceTopic, String messageKey, String payload, String reason) {
        this.sourceTopic = sourceTopic;
        this.messageKey = messageKey;
        this.payload = truncate(payload, 4000);
        this.reason = truncate(reason, 1000);
        this.receivedAt = Instant.now();
    }

    private static String truncate(String value, int max) {
        if (value == null) {
            return null;
        }
        return value.length() <= max ? value : value.substring(0, max);
    }

    public Long getId() {
        return id;
    }

    public String getSourceTopic() {
        return sourceTopic;
    }

    public String getMessageKey() {
        return messageKey;
    }

    public String getPayload() {
        return payload;
    }

    public String getReason() {
        return reason;
    }

    public Instant getReceivedAt() {
        return receivedAt;
    }
}
