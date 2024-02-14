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

psql -U musicbrainz -d musicbrainz_test_artwork_indexer -f sql/create_schema.sql
psql -U musicbrainz -d musicbrainz_test_artwork_indexer -f sql/caa_functions.sql
psql -U musicbrainz -d musicbrainz_test_artwork_indexer -f sql/eaa_functions.sql
psql -U musicbrainz -d musicbrainz_test_artwork_indexer -f sql/caa_triggers.sql
psql -U musicbrainz -d musicbrainz_test_artwork_indexer -f sql/eaa_triggers.sql

exec coverage run -m unittest discover . "test_*.py"
