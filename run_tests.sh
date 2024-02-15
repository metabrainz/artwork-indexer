#!/usr/bin/env bash

set -e

cd "$(dirname "${BASH_SOURCE[0]}")"

./create_test_db.sh

exec coverage run -m unittest discover . "test_*.py"
