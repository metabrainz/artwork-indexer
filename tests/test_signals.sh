#!/usr/bin/env bash

cd "$(dirname "${BASH_SOURCE[0]}")/../"

read -r -d '' SQL <<'EOF'
SET client_min_messages TO WARNING;
INSERT INTO artwork_indexer.event_queue (id, entity_type, action, message, created)
     VALUES (1, 'release', 'noop', '{"sleep": 3}', NOW() - interval '1 minute');
EOF

psql -U musicbrainz -d musicbrainz_test_artwork_indexer -c "$SQL" -q > /dev/null

run_indexer() {
    exec python indexer.py \
        --max-wait=1 \
        --config=config.tests.ini
}

run_indexer &
run1_pid=$!

sleep 1.5

kill -TERM "$run1_pid"
wait "$run1_pid"

read -r -d '' STATE_SQL <<'EOF'
SELECT state
  FROM artwork_indexer.event_queue
 WHERE id = 1;
EOF

event_state="$(psql -U musicbrainz -d musicbrainz_test_artwork_indexer -c "$STATE_SQL" -tAq)"
if [[ "$event_state" != 'completed' ]]; then
    echo 'ERROR: Event was not completed after SIGTERM'
    exit 1
fi

read -r -d '' SQL <<'EOF'
UPDATE artwork_indexer.event_queue
   SET state = 'queued', attempts = 0
 WHERE id = 1;
EOF

psql -U musicbrainz -d musicbrainz_test_artwork_indexer -c "$SQL"

run_indexer &
run2_pid=$!

sleep 1.5

kill -INT "$run2_pid"
wait "$run2_pid"

event_state="$(psql -U musicbrainz -d musicbrainz_test_artwork_indexer -c "$STATE_SQL" -tAq)"
if [[ "$event_state" != 'completed' ]]; then
    echo 'ERROR: Event was not completed after SIGINT'
    exit 1
fi
