package dev.deekshita.notifications;

import org.springframework.data.jpa.repository.JpaRepository;

public interface DeadLetterRepository extends JpaRepository<DeadLetter, Long> {
}
