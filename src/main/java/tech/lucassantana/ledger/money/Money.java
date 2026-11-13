package tech.lucassantana.ledger.money;

import java.util.regex.Pattern;

/**
 * An amount of BRL held as integer cents (ADR 0004).
 *
 * <p>The API speaks the API Pix decimal string ({@code "500.00"}); the domain and the
 * database only see cents, so arithmetic is exact and never touches floating point.
 */
public record Money(long cents) {

    /** Same shape as API Pix {@code valor.original}: up to 10 integer digits, exactly 2 decimals. */
    private static final Pattern PIX_DECIMAL = Pattern.compile("\\d{1,10}\\.\\d{2}");

    public static Money parse(String value) {
        if (value == null || !PIX_DECIMAL.matcher(value).matches()) {
            throw new InvalidAmountException(value);
        }
        int dot = value.length() - 3;
        long reais = Long.parseLong(value, 0, dot, 10);
        long centavos = Long.parseLong(value, dot + 1, value.length(), 10);
        return new Money(reais * 100 + centavos);
    }

    public boolean isPositive() {
        return cents > 0;
    }

    /** Pix decimal string; negative only for the settlement account balance. */
    @Override
    public String toString() {
        long abs = Math.absExact(cents);
        return (cents < 0 ? "-" : "") + abs / 100 + "." + (abs % 100 < 10 ? "0" : "") + abs % 100;
    }
}
