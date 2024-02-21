#!/usr/bin/env bash

set -e

if [[ ! -v DROPDB_COMMAND ]]; then
    DROPDB_COMMAND="sudo -u postgres dropdb"
fi

if [[ ! -v POSTGRES_SUPERUSER ]]; then
    POSTGRES_SUPERUSER=postgres
fi

$DROPDB_COMMAND --if-exists musicbrainz_test_artwork_indexer
createdb -O musicbrainz -T musicbrainz_test -U "$POSTGRES_SUPERUSER" musicbrainz_test_artwork_indexer

python indexer.py --config=config.tests.ini --setup-schema
