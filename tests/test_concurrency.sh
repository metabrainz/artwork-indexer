#!/usr/bin/env bash

cd "$(dirname "${BASH_SOURCE[0]}")/../"

read -r -d '' SQL <<'EOF'
SET client_min_messages TO WARNING;
INSERT INTO artwork_indexer.event_queue (id, entity_type, action, message, created)
     VALUES (1, 'release', 'noop', '{"run": 1}', NOW() - interval '2 minutes'),
            (2, 'release', 'noop', '{"run": 2}', NOW() - interval '1 minute');
EOF

psql -U musicbrainz -d musicbrainz_test_artwork_indexer -c "$SQL" -q > /dev/null

run_indexer() {
    poetry run python indexer.py \
        --max-wait=1 \
        --max-idle-loops=1 \
        --config=config.tests.ini 2>&1
}

run_indexer > /tmp/a26a73c_run1_output &
run1_pid=$!

# This is paired with `time.sleep(0.25)` in indexer.py. That sleep duration
# is divided by two here in order to
#  1. give run #1 enough time to select an event (which should lock it), and
#  2. give run #2 enough time to select an event while run #1 still holds a
#     lock on the first event (or at least that's what we're verifying).
sleep 0.125

run_indexer > /tmp/a26a73c_run2_output &
run2_pid=$!

wait "$run1_pid" "$run2_pid"

run1_id1_completion_count="$(cat /tmp/a26a73c_run1_output | grep -Fo 'Event id=1 completed succesfully' | wc -l)"
run2_id1_completion_count="$(cat /tmp/a26a73c_run2_output | grep -Fo 'Event id=1 completed succesfully' | wc -l)"

run1_id2_completion_count="$(cat /tmp/a26a73c_run1_output | grep -Fo 'Event id=2 completed succesfully' | wc -l)"
run2_id2_completion_count="$(cat /tmp/a26a73c_run2_output | grep -Fo 'Event id=2 completed succesfully' | wc -l)"

id1_completion_count="$(( run1_id1_completion_count +  run2_id1_completion_count ))"
id2_completion_count="$(( run1_id2_completion_count +  run2_id2_completion_count ))"

rm /tmp/a26a73c*

if [[ $id1_completion_count -gt 1 ]]; then
    echo 'ERROR: Event id=1 was processed more than once'
    exit 1
fi

if [[ $id2_completion_count -gt 1 ]]; then
    echo 'ERROR: Event id=2 was processed more than once'
    exit 1
fi

read -r -d '' SQL <<'EOF'
SET client_min_messages TO WARNING;
TRUNCATE artwork_indexer.event_queue CASCADE;
SELECT setval('artwork_indexer.event_queue_id_seq', 1, FALSE);
EOF

psql -U musicbrainz -d musicbrainz_test_artwork_indexer -c "$SQL" -q > /dev/null
