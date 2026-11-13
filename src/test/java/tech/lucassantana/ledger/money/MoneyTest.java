package tech.lucassantana.ledger.money;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class MoneyTest {

    private static final long MAX_PIX_CENTS = 999_999_999_999L; // "9999999999.99"

    @ParameterizedTest
    @CsvSource({
        "0.01, 1",
        "12.34, 1234",
        "500.00, 50000",
        "0001.00, 100",
        "9999999999.99, 999999999999"
    })
    void parsesThePixDecimalIntoCents(String value, long cents) {
        assertThat(Money.parse(value).cents()).isEqualTo(cents);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {
        "500", "500.0", "500.000", "500,00", "-1.00", "+1.00", " 1.00", "1.00 ", ".50", "1e2.00", "12345678901.00"
    })
    void rejectsAnythingOutsideThePixPattern(String value) {
        assertThatThrownBy(() -> Money.parse(value)).isInstanceOf(InvalidAmountException.class);
    }

    /** Property: format then parse returns the same cents. Seeded, so a failure reproduces (ADR 0009). */
    @Test
    void formatAndParseRoundTripForAnyValidAmount() {
        Random random = new Random(20261113L);
        for (int i = 0; i < 100_000; i++) {
            long cents = random.nextLong(0, MAX_PIX_CENTS + 1);
            assertThat(Money.parse(new Money(cents).toString()).cents()).isEqualTo(cents);
        }
        assertThat(Money.parse(new Money(MAX_PIX_CENTS).toString()).cents()).isEqualTo(MAX_PIX_CENTS);
    }

    @Test
    void formatsSmallAndNegativeAmounts() {
        assertThat(new Money(7)).hasToString("0.07");
        assertThat(new Money(1234)).hasToString("12.34");
        assertThat(new Money(-5)).hasToString("-0.05");
        assertThat(new Money(-123_456)).hasToString("-1234.56");
    }
}
