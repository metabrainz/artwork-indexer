#!/bin/bash

set -e

sudo -u postgres dropdb musicbrainz_test_artwork_indexer
createdb -O musicbrainz -T musicbrainz_test -U postgres musicbrainz_test_artwork_indexer

psql -U musicbrainz -d musicbrainz_test_artwork_indexer -f sql/create.sql
psql -U musicbrainz -d musicbrainz_test_artwork_indexer -f sql/caa_functions.sql
psql -U musicbrainz -d musicbrainz_test_artwork_indexer -f sql/eaa_functions.sql
psql -U musicbrainz -d musicbrainz_test_artwork_indexer -f sql/caa_triggers.sql
psql -U musicbrainz -d musicbrainz_test_artwork_indexer -f sql/eaa_triggers.sql

exec python tests.py
