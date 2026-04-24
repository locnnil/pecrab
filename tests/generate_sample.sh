#!/usr/bin/env bash
# Generate an integration-test sample pair for PECrab.
#
# Usage:
#   generate_sample.sh <sample_name> <num_users> <deposits_per_user>
#
# Arguments:
#   name               Sample name including numeric prefix, e.g. "20_large_test".
#                      Produces:
#                        tests/data/sample_<sample_name>.csv (input)
#                        tests/data/sample_<prefix>_out.csv  (output)
#                      where <prefix> is everything before the first '_'.
#   num_users          Number of client accounts (1 – 65535, u16).
#   deposits_per_user  Number of deposits per client (positive integer).
#                      Total tx count (num_users × deposits_per_user) must not
#                      exceed 4294967295 (u32 max).
#
# The generated input has deposits interleaved round-robin across clients so
# that all clients appear from the start of the file rather than in blocks.
# Each deposit amount is 10.0, so each client's final available balance is
# deposits_per_user × 10.0.
#
# Example:
#   ./tests/generate_sample.sh 20_big_test 500 2000
#   # → tests/data/sample_20_big_test.csv   (1 000 001 lines)
#   # → tests/data/sample_20_out.csv        (501 lines)

# Helper color hashtable
declare -A COLOR
COLOR['END']='\033[0m'
COLOR['RED']='\033[0;91m'
COLOR['GREEN']='\033[0;92m'
COLOR['NC']='\033[0m'

# Print message with a given color
printc() {
  local color="$1"
  shift
  echo -e "${COLOR[$color]}$@${COLOR['END']}"
}

# Exit with error message
exit_error() {
  printc "RED" "Error: ${*}" >&2
  exit 1
}

help() {
  cat >&2 <<'EOF'
Usage:
  generate_sample.sh <name> <num_users> <deposits_per_user>

Arguments:
  name                 Sample name (e.g. "20_large_test")
                       Generates:
                         tests/data/sample_<name>.csv
                         tests/data/sample_<prefix>_out.csv

  num_users            Number of clients (1–65535)

  deposits_per_user    Deposits per client (> 0)

Constraints:
  num_users × deposits_per_user ≤ 4294967295

Notes:
  - Transactions are shuffled using `shuf`
  - `shuf` loads all rows into memory (high RAM usage for large datasets)

Example:
  ./tests/generate_sample.sh 20_big_test 500 2000
EOF
  exit 1
}

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$SCRIPT_DIR/data"

if [[ $# -ne 3 ]]; then
  help
fi

name="$1"
num_users="$2"
deposits_per_user="$3"

if ! [[ "$num_users" =~ ^[0-9]+$ ]] || ((num_users < 1 || num_users > 65535)); then
  exit_error "error: num_users must be an integer in 1–65535 (got '$num_users')"
fi

if ! [[ "$deposits_per_user" =~ ^[0-9]+$ ]] || ((deposits_per_user < 1)); then
  exit_error "error: deposits_per_user must be a positive integer (got '$deposits_per_user')"
fi

total_tx=$((num_users * deposits_per_user))
if ((total_tx > 4294967295)); then
  exit_error "error: num_users × deposits_per_user = $total_tx exceeds u32 max (4294967295)"
fi

# Setup
num_prefix="${name%%_*}"
if [[ ! $num_prefix =~ ^[0-9]+$ ]]; then
  exit_error "Invalid number prefix for sample name: $num_prefix; expected: <number>_<description>"
fi

input_file="$DATA_DIR/sample_${name}.csv"
output_file="$DATA_DIR/sample_${num_prefix}_out.csv"

mkdir -p "$DATA_DIR"

# Input CSV
#
# tx IDs run 1..total_tx. Round-robin assignment:
#   client = (tx_id - 1) % num_users + 1
#
# seq streams one integer per line; awk maps each to a CSV row, then shuf
# randomises the row order. The header is printed before the pipe so it
# stays on line 1. shuf buffers all rows in memory to perform a uniform
# Fisher-Yates shuffle — expected memory use is ~25 bytes × total_tx.
{
  echo "type,client,tx,amount"
  seq 1 "$total_tx" | awk -v N="$num_users" '{
        client = (($1 - 1) % N) + 1
        printf "deposit,%d,%d,10.0000\n", client, $1
    }' | shuf
} >"$input_file"

# Expected output CSV
#
# Every client receives exactly deposits_per_user deposits of 10.0 each.
# Integer arithmetic avoids floating-point rounding for the total.
{
  echo "client,available,held,total,locked"
  seq 1 "$num_users" | awk -v D="$deposits_per_user" '{
        total = D * 10
        printf "%d,%d.0,0.0,%d.0,false\n", $1, total, total
    }'
} >"$output_file"

# Summary
input_lines=$(wc -l <"$input_file")
output_lines=$(wc -l <"$output_file")

echo "Generated:"
printf "  Input:  %s  (%d lines)\n" "$input_file" "$input_lines"
printf "  Output: %s (%d lines)\n" "$output_file" "$output_lines"
