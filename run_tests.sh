#!/usr/bin/env bash

set -e

cd "$(dirname "${BASH_SOURCE[0]}")"

./create_test_db.sh

./tests/test_concurrency.sh
./tests/test_signals.sh

exec poetry run coverage run -m unittest discover . "test_*.py"
