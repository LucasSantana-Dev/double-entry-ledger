package tech.lucassantana.ledger;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DoubleEntryLedgerApplication {

	public static void main(String[] args) {
		SpringApplication.run(DoubleEntryLedgerApplication.class, args);
	}

}
