import os.path
import unittest
from textwrap import dedent
import indexer
from . import (
    MockResponse,
    TestArtArchive,
    index_event,
    tests_config,
)


RELEASE1_MBID = '16ebbc86-670c-4ad3-980b-bfbd1eee4ff4'


class TestGeneral(TestArtArchive):

    def setUp(self):
        super().setUp()

        with open(
            os.path.join(os.path.dirname(__file__), 'caa_setup.sql'),
            'r'
        ) as fp:
            self.pg_conn.execute_and_commit(fp.read())

    def tearDown(self):
        with open(
            os.path.join(os.path.dirname(__file__), 'caa_teardown.sql'),
            'r'
        ) as fp:
            self.pg_conn.execute_and_commit(fp.read())
        super().tearDown()

    def test_duplicate_updates(self):
        self.pg_conn.execute_and_commit(dedent('''
            UPDATE musicbrainz.release SET name = 'update' WHERE id = 1;
            UPDATE cover_art_archive.cover_art SET comment = 'a' WHERE id = 1;
            UPDATE cover_art_archive.cover_art SET comment = 'b' WHERE id = 1;
        '''))

        # Test that duplicate index events are not inserted.
        self.assertEqual(self.get_event_queue(), [
            index_event(RELEASE1_MBID, entity_type='release', id=1)
        ])

    def test_cleanup(self):
        self.pg_conn.execute_and_commit(dedent('''
            UPDATE release SET name = 'updated name1' WHERE id = 1;
        '''))

        self.session.next_responses = [
            MockResponse(),
            MockResponse(),
            MockResponse(),
        ]

        indexer.indexer(tests_config, self.pg_conn, 1,
                        max_idle_loops=1,
                        http_client_cls=self.http_client_cls)

        event_count = self.pg_conn.execute(dedent('''
            SELECT count(*) as count FROM artwork_indexer.event_queue
        ''')).fetchone()['count']
        self.assertEqual(event_count, 1)

        self.pg_conn.execute_and_commit(dedent('''
            UPDATE artwork_indexer.event_queue
            SET created = (created - interval '100 days');
        '''))

        indexer.indexer(tests_config, self.pg_conn, 2,
                        max_idle_loops=2,
                        http_client_cls=self.http_client_cls)

        event_count = self.pg_conn.execute(dedent('''
            SELECT count(*) as count FROM artwork_indexer.event_queue
        ''')).fetchone()['count']
        self.assertEqual(event_count, 0)

    def test_depends_on(self):
        self.pg_conn.execute_and_commit(dedent('''
            INSERT INTO artwork_indexer.event_queue
                    (id, state, entity_type, action, depends_on, message,
                     created)
                 VALUES (1, 'queued', 'release', 'index', NULL,
                         '{"gid": "A"}', NOW() - interval '1 day'),
                        (2, 'completed', 'release', 'index', NULL,
                         '{"gid": "B"}', NOW() - interval '2 days'),
                        (3, 'queued', 'release', 'index', '{1,2}',
                         '{"gid": "C"}', NOW() - interval '3 day');
        '''))
        next_event = indexer.get_next_event(self.pg_conn)
        # Event #3, even though it was created the earliest, still depends
        # on event #1, which is not yet completed.
        self.assertEqual(next_event['id'], 1)

    def test_failure(self):
        self.pg_conn.execute_and_commit(dedent('''
            INSERT INTO artwork_indexer.event_queue
                    (id, entity_type, action, message, depends_on, created)
                 VALUES (1, 'release', 'noop', '{"fail": true}', NULL,
                         NOW() - interval '1 day'),
                        (2, 'release', 'noop', '{"id": 2}', '{1}',
                         NOW() - interval '1 day'),
                        (3, 'release', 'noop', '{"id": 3}', '{2}',
                         NOW() - interval '1 day');
        '''))

        for index in range(0, indexer.MAX_ATTEMPTS):
            self.pg_conn.execute_and_commit(dedent('''
                UPDATE artwork_indexer.event_queue
                   SET last_updated =
                        (NOW() - (interval '1 hour' * attempts))
                 WHERE id = 1;
            '''))

            indexer.indexer(tests_config, self.pg_conn, 1,
                            max_idle_loops=1,
                            http_client_cls=self.http_client_cls)

            pg_cur = self.pg_conn.execute(
                'SELECT * FROM artwork_indexer.event_queue '
                'WHERE id IN (1, 2, 3) ORDER BY id'
            )
            event1, event2, event3 = pg_cur.fetchall()
            attempts = event1['attempts']

            self.assertEqual(attempts, index + 1)

            pg_cur = self.pg_conn.execute(dedent('''
                SELECT count(*) AS count
                  FROM artwork_indexer.event_failure_reason
                 WHERE event = 1
                   AND failure_reason = 'Failure (no-op)'
            '''))
            failure_reason_count = pg_cur.fetchone()['count']
            self.assertEqual(failure_reason_count, index + 1)

            if attempts < indexer.MAX_ATTEMPTS:
                self.assertEqual(event1['state'], 'queued')
                self.assertEqual(event2['state'], 'queued')
                self.assertEqual(event3['state'], 'queued')
            else:
                self.assertEqual(event1['state'], 'failed')
                self.assertEqual(event2['state'], 'failed')
                self.assertEqual(event3['state'], 'failed')
                event2_failure_reason, event3_failure_reason = \
                    self.pg_conn.execute(dedent('''
                        SELECT failure_reason
                          FROM artwork_indexer.event_failure_reason
                         WHERE event IN (2, 3)
                         ORDER BY event
                    ''')).fetchall()
                self.assertEqual(
                    event2_failure_reason['failure_reason'],
                    'This event was marked as failed because an event it ' +
                    'depended on (1) had failed.',
                )
                self.assertEqual(
                    event3_failure_reason['failure_reason'],
                    'This event was marked as failed because an event it ' +
                    'depended on (1) had failed.',
                )

        # Should not return the failed event.
        self.assertEqual(indexer.get_next_event(self.pg_conn), None)

    def test_timeout(self):
        self.pg_conn.execute_and_commit(dedent('''
            INSERT INTO artwork_indexer.event_queue
                    (id, state, entity_type, action, message, created)
                 VALUES (1, 'running', 'release', 'index',
                         '{"gid": "A"}', NOW() - interval '5 minutes');
        '''))

        indexer.indexer(tests_config, self.pg_conn, 1,
                        max_idle_loops=1,
                        http_client_cls=self.http_client_cls)

        event = self.pg_conn.execute(dedent('''
            SELECT eq.*,
                   string_agg(efr.failure_reason, ', ') AS failure_reason
              FROM artwork_indexer.event_queue eq
              JOIN artwork_indexer.event_failure_reason efr
                ON efr.event = eq.id
             WHERE eq.id = 1
          GROUP BY eq.id;
        ''')).fetchone()

        self.assertEqual(event['state'], 'failed')
        self.assertRegex(
            event['failure_reason'],
            r'been running for more than 2\.5 minutes'
        )


if __name__ == '__main__':
    unittest.main(verbosity=2)
