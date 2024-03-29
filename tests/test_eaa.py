import os.path
import unittest
from textwrap import dedent
import indexer
from projects import EAA_PROJECT
from . import (
    MockResponse,
    TestArtArchive,
    image_copy_put,
    index_event,
    index_json_put,
    mb_metadata_xml_get,
    mb_metadata_xml_put,
    tests_config,
)


EVENT_XML_TEMPLATE = (
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    '<metadata xmlns="http://musicbrainz.org/ns/mmd-2.0#">'
        '<event id="{mbid}" type="{type_name}" type-id="{type_id}">'
            '<name>{name}</name>'
            '<life-span>'
                '<begin>{begin}</begin>'
                '<end>{end}</end>'
            '</life-span>'
            '<time>{time}</time>'
        '</event>'
    '</metadata>\n'
)


def event_index_event(mbid, **kwargs):
    return index_event(mbid, entity_type='event', **kwargs)


def event_index_json_put(mbid, images):
    return index_json_put(EAA_PROJECT, mbid, images)


def event_mb_metadata_xml_get(mbid):
    return mb_metadata_xml_get(EAA_PROJECT, mbid)


def event_mb_metadata_xml_put(mbid, xml):
    return mb_metadata_xml_put(EAA_PROJECT, mbid, xml)


def event_image_copy_put(source_mbid, target_mbid, image_id):
    return image_copy_put(EAA_PROJECT, source_mbid, target_mbid, image_id)


EVENT1_MBID = 'e2aad65a-12e0-44ec-b693-94d225154e90'
EVENT2_MBID = 'a0f19ff3-e140-417f-81c6-2a7466eeea0a'

EVENT1_XML_FMT_ARGS = {
    'mbid': EVENT1_MBID,
    'name': 'live at the place 1',
    'type_name': 'Concert',
    'type_id': 'ef55e8d7-3d00-394a-8012-f5506a29ff0b',
    'begin': '1990',
    'end': '1990',
    'time': '20:00',
}

EVENT2_XML_FMT_ARGS = {
    'mbid': EVENT2_MBID,
    'name': 'live at the place 2',
    'type_name': 'Concert',
    'type_id': 'ef55e8d7-3d00-394a-8012-f5506a29ff0b',
    'begin': '1991',
    'end': '1991',
    'time': '21:00',
}

EVENT1_XML = EVENT_XML_TEMPLATE.format(**EVENT1_XML_FMT_ARGS)
EVENT2_XML = EVENT_XML_TEMPLATE.format(**EVENT2_XML_FMT_ARGS)


class TestEventArtArchive(TestArtArchive):

    def setUp(self):
        super().setUp()

        with open(
            os.path.join(os.path.dirname(__file__), 'eaa_setup.sql'),
            'r'
        ) as fp:
            self.pg_conn.execute_and_commit(fp.read())

        self._orig_image1_json = {
            'approved': False,
            'comment': 'hello',
            'edit': 1,
            'front': True,
            'id': 1,
            'types': ['Poster'],
        }
        self._orig_image2_json = {
            'approved': False,
            'comment': 'yes hi',
            'edit': 2,
            'front': False,
            'id': 2,
            'types': [],
        }

    def tearDown(self):
        with open(
            os.path.join(os.path.dirname(__file__), 'eaa_teardown.sql'),
            'r'
        ) as fp:
            self.pg_conn.execute_and_commit(fp.read())
        super().tearDown()

    def _event_reindex_test(self,
                            event_mbid=None,
                            event_id=None,
                            images_json=None,
                            xml_fmt_args_base=None,
                            xml_fmt_args=None):
        self.assertEqual(self.get_event_queue(), [
            event_index_event(event_mbid, id=event_id),
        ])

        xml = EVENT_XML_TEMPLATE.format(
            **(xml_fmt_args_base | (xml_fmt_args or {}))
        )

        self.session.last_requests = []
        self.session.next_responses = [
            MockResponse(),
            MockResponse(status=200, content=xml),
            MockResponse(status=200, content=xml),
        ]

        indexer.indexer(tests_config, self.pg_conn, 1,
                        max_idle_loops=1,
                        http_client_cls=self.http_client_cls)

        self.assertEqual(self.session.last_requests, [
            event_index_json_put(event_mbid, images_json),
            event_mb_metadata_xml_get(event_mbid),
            event_mb_metadata_xml_put(event_mbid, xml),
        ])

    def _event1_reindex_test(self,
                             event_id=None,
                             images_json=None,
                             xml_fmt_args=None):
        self._event_reindex_test(
            event_mbid=EVENT1_MBID,
            event_id=event_id,
            images_json=images_json,
            xml_fmt_args_base=EVENT1_XML_FMT_ARGS,
            xml_fmt_args=xml_fmt_args
        )

    def test_inserting_event_art(self):
        # artwork_indexer_a_ins_event_art

        self.pg_conn.execute_and_commit(dedent('''
            INSERT INTO musicbrainz.edit
                    (id, editor, type, status, expire_time)
                VALUES (3, 10, 158, 2, now());

            INSERT INTO musicbrainz.edit_data (edit, data)
                VALUES (3, '{}');

            INSERT INTO event_art_archive.event_art
                    (id, event, mime_type, edit, ordering, comment)
                VALUES (3, 1, 'image/jpeg', 3, 1, '?');

            INSERT INTO event_art_archive.event_art_type (id, type_id)
                VALUES (3, 1);
        '''))

        new_image3_json = {
            'approved': False,
            'comment': '?',
            'edit': 3,
            'front': False,
            'id': 3,
            'types': ['Poster'],
        }

        self._event1_reindex_test(
            event_id=1,
            images_json=[self._orig_image1_json, new_image3_json],
        )

    def test_updating_event_art(self):
        # artwork_indexer_a_upd_event_art

        self.pg_conn.execute_and_commit(dedent('''
            UPDATE event_art_archive.event_art
                SET ordering = 3, comment = ''
                WHERE id = 1
        '''))

        new_image1_json = self._orig_image1_json | {'comment': ''}

        self._event1_reindex_test(
            event_id=1,
            images_json=[new_image1_json],
        )

    def test_deleting_event_art(self):
        # artwork_indexer_a_del_event_art

        self.pg_conn.execute_and_commit(dedent('''
            DELETE FROM event_art_archive.event_art
                WHERE id = 1
        '''))

        self.assertEqual(self.get_event_queue(), [
            {
                'id': 1,
                'state': 'queued',
                'entity_type': 'event',
                'action': 'delete_image',
                'message': {
                    'artwork_id': 1,
                    'gid': EVENT1_MBID,
                    'suffix': 'jpg',
                },
                'depends_on': None,
                'attempts': 0,
            },
            event_index_event(EVENT1_MBID, id=2, depends_on=[1]),
        ])

        self.session.last_requests = []
        self.session.next_responses = [
            MockResponse(status=204),
            MockResponse(),
            MockResponse(status=200, content=EVENT1_XML),
            MockResponse(status=200, content=EVENT1_XML),
        ]

        indexer.indexer(tests_config, self.pg_conn, 1,
                        max_idle_loops=1,
                        http_client_cls=self.http_client_cls)

        self.assertEqual(self.session.last_requests, [
            {
                'method': 'DELETE',
                'url': f'http://mbid-{EVENT1_MBID}.s3.example.com/' +
                       f'mbid-{EVENT1_MBID}-1.jpg',
                'data': None,
                'headers': {
                    'authorization': 'LOW user:pass',
                    'x-archive-cascade-delete': '1',
                    'x-archive-keep-old-version': '1',
                },
            },
            event_index_json_put(EVENT1_MBID, []),
            event_mb_metadata_xml_get(EVENT1_MBID),
            event_mb_metadata_xml_put(EVENT1_MBID, EVENT1_XML),
        ])

    def test_merging_events(self):
        # artwork_indexer_a_del_event

        # Queue an index event (via artwork_indexer_a_upd_event_art).
        # We're checking that it's replaced by the following event
        # (entity) deletion event.
        self.pg_conn.execute_and_commit(dedent('''
            UPDATE event_art_archive.event_art
                SET ordering = 3, comment = ''
                WHERE id = 1
        '''))

        new_image1_json = self._orig_image1_json | {'comment': ''}

        self.assertEqual(self.get_event_queue(), [
            event_index_event(EVENT1_MBID, id=1),
        ])

        # This simulates a merge, where the cover art is first copied to
        # another release, and the original release is deleted.
        self.pg_conn.execute_and_commit(dedent('''
            UPDATE event_art_archive.event_art SET event = 2 WHERE id = 1;
            UPDATE event_meta SET event_art_presence = 'present' WHERE id = 2;
            DELETE FROM event WHERE id = 1;
        '''))

        self.assertEqual(self.get_event_queue(), [
            {
                'id': 5,
                'state': 'queued',
                'entity_type': 'event',
                'action': 'copy_image',
                'message': {
                    'artwork_id': 1,
                    'new_gid': EVENT2_MBID,
                    'old_gid': EVENT1_MBID,
                    'suffix': 'jpg',
                },
                'depends_on': None,
                'attempts': 0,
            },
            {
                'id': 6,
                'state': 'queued',
                'entity_type': 'event',
                'action': 'delete_image',
                'message': {
                    'artwork_id': 1,
                    'gid': EVENT1_MBID,
                    'suffix': 'jpg',
                },
                'depends_on': [5],
                'attempts': 0,
            },
            event_index_event(EVENT2_MBID, id=7, depends_on=[6]),
            {
                'id': 8,
                'state': 'queued',
                'entity_type': 'event',
                'action': 'deindex',
                'message': {'gid': EVENT1_MBID},
                'depends_on': [6],
                'attempts': 0,
            },
        ])

        # Make the copy fail. This should halt processing of all dependant
        # events (delete_image, index).
        self.session.last_requests = []
        self.session.next_responses = [
            MockResponse(status=400),
            MockResponse(status=204),
        ]

        self.pg_conn.execute_and_commit(dedent('''
            UPDATE artwork_indexer.event_queue
            SET attempts = 4, last_updated = (now() - interval '1 day')
            WHERE action = 'copy_image'
        '''))

        indexer.indexer(tests_config, self.pg_conn, 1,
                        max_idle_loops=1,
                        http_client_cls=self.http_client_cls)

        self.assertEqual(self.get_event_queue(), [
            {
                'id': 5,
                'state': 'failed',
                'entity_type': 'event',
                'action': 'copy_image',
                'message': {
                    'artwork_id': 1,
                    'new_gid': EVENT2_MBID,
                    'old_gid': EVENT1_MBID,
                    'suffix': 'jpg',
                },
                'depends_on': None,
                'attempts': 5,
            },
            {
                'id': 6,
                'state': 'queued',
                'entity_type': 'event',
                'action': 'delete_image',
                'message': {
                    'artwork_id': 1,
                    'gid': EVENT1_MBID,
                    'suffix': 'jpg',
                },
                'depends_on': [5],
                'attempts': 0,
            },
            event_index_event(EVENT2_MBID, id=7, depends_on=[6]),
            {
                'id': 8,
                'state': 'queued',
                'entity_type': 'event',
                'action': 'deindex',
                'message': {'gid': EVENT1_MBID},
                'depends_on': [6],
                'attempts': 0,
            },
        ])

        self.assertEqual(
            self.pg_conn.execute(dedent('''
                SELECT failure_reason
                FROM artwork_indexer.event_failure_reason
                WHERE event = 5
            ''')).fetchone()['failure_reason'],
            'HTTP 400',
        )

        self.assertEqual(self.session.last_requests, [
            event_image_copy_put(EVENT1_MBID, EVENT2_MBID, 1),
        ])

        # Revert the artificial failure we created, which should unblock
        # processing of the failed event and its dependants.
        self.pg_conn.execute_and_commit(dedent('''
            UPDATE artwork_indexer.event_queue
            SET attempts = 0, state = 'queued'
            WHERE action = 'copy_image'
        '''))

        self.session.last_requests = []
        self.session.next_responses = [
            MockResponse(),
            MockResponse(status=204),
            MockResponse(),
            MockResponse(status=200, content=EVENT2_XML),
            MockResponse(status=200, content=EVENT2_XML),
            MockResponse(),
        ]

        indexer.indexer(tests_config, self.pg_conn, 1,
                        max_idle_loops=1,
                        http_client_cls=self.http_client_cls)

        self.assertEqual(self.session.last_requests, [
            event_image_copy_put(EVENT1_MBID, EVENT2_MBID, 1),
            {
                'method': 'DELETE',
                'url': f'http://mbid-{EVENT1_MBID}.s3.example.com/' +
                       f'mbid-{EVENT1_MBID}-1.jpg',
                'data': None,
                'headers': {
                    'authorization': 'LOW user:pass',
                    'x-archive-cascade-delete': '1',
                    'x-archive-keep-old-version': '1',
                },
            },
            event_index_json_put(EVENT2_MBID, [
                self._orig_image2_json,
                new_image1_json,
            ]),
            event_mb_metadata_xml_get(EVENT2_MBID),
            event_mb_metadata_xml_put(EVENT2_MBID, EVENT2_XML),
            {
                'method': 'DELETE',
                'url': f'http://mbid-{EVENT1_MBID}.s3.example.com/index.json',
                'headers': {
                    'authorization': 'LOW user:pass',
                    'x-archive-keep-old-version': '1',
                    'x-archive-cascade-delete': '1',
                },
                'data': None,
            },
        ])

    def test_deleting_event_with_artwork(self):
        # Deleting an event with artwork should queue `delete_image` and
        # `deindex` events.

        self.pg_conn.execute_and_commit(dedent('''
            DELETE FROM event WHERE id = 1;
        '''))

        self.assertEqual(self.get_event_queue(), [
            {
                'id': 1,
                'state': 'queued',
                'entity_type': 'event',
                'action': 'delete_image',
                'message': {
                    'artwork_id': 1,
                    'gid': EVENT1_MBID,
                    'suffix': 'jpg'
                },
                'depends_on': None,
                'attempts': 0,
            },
            {
                'id': 2,
                'state': 'queued',
                'entity_type': 'event',
                'action': 'deindex',
                'message': {'gid': EVENT1_MBID},
                'depends_on': None,
                'attempts': 0,
            },
        ])

    def test_deleting_event_without_artwork(self):
        # Deleting an event with no artwork should not queue a deindex.

        self.pg_conn.execute_and_commit(dedent('''
            DELETE FROM event WHERE id = 3;
        '''))

        self.assertEqual(self.get_event_queue(), [])

    def test_inserting_event_art_type(self):
        # artwork_indexer_a_ins_event_art_type

        self.pg_conn.execute_and_commit(dedent('''
            INSERT INTO event_art_archive.event_art_type (id, type_id)
                VALUES (2, 1);
        '''))

        new_image2_json = (
            self._orig_image2_json |
            {'front': True, 'types': ['Poster']}
        )

        self._event_reindex_test(
            event_mbid=EVENT2_MBID,
            event_id=1,
            images_json=[new_image2_json],
            xml_fmt_args_base=EVENT2_XML_FMT_ARGS,
        )

    def test_deleting_event_art_type(self):
        # artwork_indexer_a_del_event_art_type

        self.pg_conn.execute_and_commit(dedent('''
            DELETE FROM event_art_archive.event_art_type
                WHERE id = 1 AND type_id = 1
        '''))

        new_image1_json = self._orig_image1_json | \
            {'front': False, 'types': []}

        self._event1_reindex_test(
            event_id=1,
            images_json=[new_image1_json],
        )


if __name__ == '__main__':
    unittest.main(verbosity=2)
