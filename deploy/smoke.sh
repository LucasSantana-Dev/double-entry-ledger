#!/usr/bin/env bash
# Smoke test of a running deployment (ADR 0024): two accounts, a deposit, a transfer, the
# balances over HTTP, then the transfer's statement lines in the database, which arrive
# asynchronously through the outbox, Kafka and the statement consumer.
#
#   BASE_URL   where the API answers (default http://localhost:8080)
#   SQL_CMD    a command that runs one SQL statement given as its last argument and prints the
#              bare result (default: psql inside the postgres pod of the kind deployment)
#   STATEMENT_TIMEOUT_SECONDS  how long to wait for the statement lines (default 60)
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
SQL_CMD="${SQL_CMD:-kubectl -n ledger exec postgres-0 -- psql -U ledger -d ledger -tAc}"
STATEMENT_TIMEOUT_SECONDS="${STATEMENT_TIMEOUT_SECONDS:-60}"
RUN_ID="smoke-$(date +%s)-$RANDOM"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

# Prints the string value of a top-level JSON field.
field() {
  sed -n "s/.*\"$1\":\"\([^\"]*\)\".*/\1/p"
}

# POST with an Idempotency-Key; prints the body, fails on any status but 201.
post() {
  local path="$1" key="$2" body="$3" response status
  response=$(curl -sS -w '\n%{http_code}' -X POST "$BASE_URL$path" \
    -H "Idempotency-Key: $key" -H 'Content-Type: application/json' -d "$body")
  status="${response##*$'\n'}"
  response="${response%$'\n'*}"
  [ "$status" = 201 ] || fail "POST $path returned $status: $response"
  echo "$response"
}

balance() {
  curl -sS -f "$BASE_URL/accounts/$1" | field balance
}

a=$(post /accounts "$RUN_ID-a" '' | field id)
b=$(post /accounts "$RUN_ID-b" '' | field id)
[ -n "$a" ] && [ -n "$b" ] || fail "account ids missing"
echo "accounts: $a $b"

post /deposits "$RUN_ID-deposit" "{\"accountId\":\"$a\",\"amount\":\"100.00\"}" > /dev/null
transfer=$(post /transfers "$RUN_ID-transfer" \
  "{\"fromAccountId\":\"$a\",\"toAccountId\":\"$b\",\"amount\":\"30.00\"}" | field id)
[ -n "$transfer" ] || fail "transfer id missing"
echo "transfer: $transfer"

[ "$(balance "$a")" = "70.00" ] || fail "balance of $a is $(balance "$a"), expected 70.00"
[ "$(balance "$b")" = "30.00" ] || fail "balance of $b is $(balance "$b"), expected 30.00"
echo "balances: 70.00 and 30.00"

query="select count(*) from statement.statement_lines where transfer_id = '$transfer'"
deadline=$((SECONDS + STATEMENT_TIMEOUT_SECONDS))
while true; do
  lines=$($SQL_CMD "$query" | tr -d '[:space:]')
  [ "$lines" = 2 ] && break
  [ "$SECONDS" -lt "$deadline" ] || fail "transfer $transfer has $lines statement lines after ${STATEMENT_TIMEOUT_SECONDS}s, expected 2"
  sleep 2
done
echo "statement: 2 lines for transfer $transfer"
echo "smoke test passed"
