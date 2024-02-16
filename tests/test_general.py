import os.path
import unittest
from textwrap import dedent
import indexer
from . import (
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
            self.pg_conn.execute(fp.read())

    def tearDown(self):
        with open(
            os.path.join(os.path.dirname(__file__), 'caa_teardown.sql'),
            'r'
        ) as fp:
            self.pg_conn.execute(fp.read())
        super().tearDown()

    def test_duplicate_updates(self):
        self.pg_conn.execute(dedent('''
            UPDATE musicbrainz.release SET name = 'update' WHERE id = 1;
            UPDATE cover_art_archive.cover_art SET comment = 'a' WHERE id = 1;
            UPDATE cover_art_archive.cover_art SET comment = 'b' WHERE id = 1;
        '''))

        # Test that duplicate index events are not inserted.
        self.assertEqual(self.get_event_queue(), [
            index_event(RELEASE1_MBID, entity_type='release', id=1)
        ])

    def test_cleanup(self):
        self.pg_conn.execute(dedent('''
            UPDATE release SET name = 'updated name1' WHERE id = 1;
        '''))

        indexer.indexer(tests_config, 1,
                        max_idle_loops=1,
                        http_client_cls=self.http_client_cls)

        event_count = self.pg_conn.execute(dedent('''
            SELECT count(*) as count FROM artwork_indexer.event_queue
        ''')).fetchone()['count']
        self.assertEqual(event_count, 1)

        self.pg_conn.execute(dedent('''
            UPDATE artwork_indexer.event_queue
            SET created = (created - interval '90 days');
        '''))

        indexer.indexer(tests_config, 2,
                        max_idle_loops=2,
                        http_client_cls=self.http_client_cls)

        event_count = self.pg_conn.execute(dedent('''
            SELECT count(*) as count FROM artwork_indexer.event_queue
        ''')).fetchone()['count']
        self.assertEqual(event_count, 0)

    def test_depends_on(self):
        self.pg_conn.execute(dedent('''
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


if __name__ == '__main__':
    unittest.main(verbosity=2)
