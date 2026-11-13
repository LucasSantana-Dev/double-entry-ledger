package tech.lucassantana.ledger.transfer;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

interface EntryRepository extends JpaRepository<Entry, UUID> {
}
