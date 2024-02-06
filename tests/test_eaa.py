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

    async def asyncSetUp(self):
        await super().asyncSetUp()

        await self.pg_conn.execute(dedent('''
            INSERT INTO musicbrainz.event
                    (id, gid, name, begin_date_year, end_date_year,
                     time, type)
                VALUES
                    (1, 'e2aad65a-12e0-44ec-b693-94d225154e90',
                     'live at the place 1', 1990, 1990, '20:00', 1),
                    (2, 'a0f19ff3-e140-417f-81c6-2a7466eeea0a',
                     'live at the place 2', 1991, 1991, '21:00', 1);

            INSERT INTO musicbrainz.editor
                    (id, name, password, ha1, email, email_confirm_date)
                VALUES
                    (10, 'Editor', '{CLEARTEXT}pass',
                     'b5ba49bbd92eb35ddb35b5acd039440d',
                     'Editor@example.com', now());

            INSERT INTO musicbrainz.edit
                    (id, editor, type, status, expire_time)
                VALUES (1, 10, 158, 2, now()),
                       (2, 10, 158, 2, now());

            INSERT INTO musicbrainz.edit_data (edit, data)
                VALUES (1, '{}'),
                       (2, '{}');

            INSERT INTO event_art_archive.event_art
                    (id, event, mime_type, edit, ordering, comment)
                VALUES (1, 1, 'image/jpeg', 1, 1, 'hello'),
                       (2, 2, 'image/jpeg', 2, 1, 'yes hi');

            INSERT INTO event_art_archive.event_art_type (id, type_id)
                VALUES (1, 1);

            TRUNCATE artwork_indexer.event_queue CASCADE;

            SELECT setval('artwork_indexer.event_queue_id_seq', 1, FALSE);
        '''))

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

    async def asyncTearDown(self):
        await self.pg_conn.execute(dedent('''
            TRUNCATE musicbrainz.event CASCADE;
            TRUNCATE musicbrainz.editor CASCADE;
        '''))
        await super().asyncTearDown()

    async def _event_reindex_test(self,
                                  event_mbid=None,
                                  event_id=None,
                                  images_json=None,
                                  xml_fmt_args_base=None,
                                  xml_fmt_args=None):
        self.session.last_requests = []

        self.assertEqual(await self.get_event_queue(), [
            event_index_event(event_mbid, id=event_id),
        ])

        await indexer.indexer(tests_config, 1,
                              max_idle_loops=1,
                              http_client_cls=self.http_client_cls)

        self.assertEqual(self.session.last_requests, [
            event_index_json_put(event_mbid, images_json),
            event_mb_metadata_xml_get(event_mbid),
            event_mb_metadata_xml_put(
                event_mbid,
                EVENT_XML_TEMPLATE.format(
                    **(xml_fmt_args_base | (xml_fmt_args or {}))
                )
            ),
        ])

    async def _event1_reindex_test(self,
                                   event_id=None,
                                   images_json=None,
                                   xml_fmt_args=None):
        await self._event_reindex_test(
            event_mbid=EVENT1_MBID,
            event_id=event_id,
            images_json=images_json,
            xml_fmt_args_base=EVENT1_XML_FMT_ARGS,
            xml_fmt_args=xml_fmt_args
        )

    async def test_inserting_event_art(self):
        # artwork_indexer_a_ins_event_art

        await self.pg_conn.execute(dedent('''
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

        await self._event1_reindex_test(
            event_id=1,
            images_json=[self._orig_image1_json, new_image3_json],
        )

    async def test_updating_event_art(self):
        # artwork_indexer_a_upd_event_art

        await self.pg_conn.execute(dedent('''
            UPDATE event_art_archive.event_art
                SET ordering = 3, comment = ''
                WHERE id = 1
        '''))

        new_image1_json = self._orig_image1_json | {'comment': ''}

        await self._event1_reindex_test(
            event_id=1,
            images_json=[new_image1_json],
        )

    async def test_deleting_event_art(self):
        # artwork_indexer_a_del_event_art

        await self.pg_conn.execute(dedent('''
            DELETE FROM event_art_archive.event_art
                WHERE id = 1
        '''))

        self.session.last_requests = []

        self.assertEqual(await self.get_event_queue(), [
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

        await indexer.indexer(tests_config, 1,
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

    async def test_deleting_event(self):
        # artwork_indexer_a_del_event

        # Queue an index event (via artwork_indexer_a_upd_event_art).
        # We're checking that it's replaced by the following event
        # (entity) deletion event.
        await self.pg_conn.execute(dedent('''
            UPDATE event_art_archive.event_art
                SET ordering = 3, comment = ''
                WHERE id = 1
        '''))

        new_image1_json = self._orig_image1_json | {'comment': ''}

        self.assertEqual(await self.get_event_queue(), [
            event_index_event(EVENT1_MBID, id=1),
        ])

        # This simulates a merge, where the cover art is first copied to
        # another release, and the original release is deleted.
        await self.pg_conn.execute(dedent('''
            UPDATE event_art_archive.event_art SET event = 2 WHERE id = 1;
            UPDATE event_meta SET event_art_presence = 'present' WHERE id = 2;
            DELETE FROM event WHERE id = 1;
        '''))

        self.assertEqual(await self.get_event_queue(), [
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
            event_index_event(EVENT2_MBID, id=8, depends_on=[6]),
            {
                'id': 9,
                'state': 'queued',
                'entity_type': 'event',
                'action': 'deindex',
                'message': {'gid': EVENT1_MBID},
                'depends_on': None,
                'attempts': 0,
            },
        ])

        # Make the copy fail. This should halt processing of all dependant
        # events (delete_image, index).
        print('note, the following test is expected to log an HTTP 400 error')
        self.session.next_responses.append(MockResponse(status=400))

        await self.pg_conn.execute(dedent('''
            UPDATE artwork_indexer.event_queue
            SET attempts = 4, last_updated = (now() - interval '1 day')
            WHERE action = 'copy_image'
        '''))

        self.session.last_requests = []
        await indexer.indexer(tests_config, 1,
                              max_idle_loops=1,
                              http_client_cls=self.http_client_cls)

        self.assertEqual(await self.get_event_queue(), [
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
            event_index_event(EVENT2_MBID, id=8, depends_on=[6]),
        ])

        self.assertEqual(
            await self.pg_conn.fetchval(dedent('''
                SELECT failure_reason
                FROM artwork_indexer.event_failure_reason
                WHERE event = 5
            ''')),
            'HTTP 400',
        )

        self.assertEqual(self.session.last_requests, [
            event_image_copy_put(EVENT1_MBID, EVENT2_MBID, 1),
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

        # Revert the artificial failure we created, which should unblock
        # processing of the failed event and its dependants.
        await self.pg_conn.execute(dedent('''
            UPDATE artwork_indexer.event_queue
            SET attempts = 0, state = 'queued'
            WHERE action = 'copy_image'
        '''))

        self.session.last_requests = []

        await indexer.indexer(tests_config, 1,
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
        ])

    async def test_inserting_event_art_type(self):
        # artwork_indexer_a_ins_event_art_type

        await self.pg_conn.execute(dedent('''
            INSERT INTO event_art_archive.event_art_type (id, type_id)
                VALUES (2, 1);
        '''))

        new_image2_json = (
            self._orig_image2_json |
            {'front': True, 'types': ['Poster']}
        )

        await self._event_reindex_test(
            event_mbid=EVENT2_MBID,
            event_id=1,
            images_json=[new_image2_json],
            xml_fmt_args_base=EVENT2_XML_FMT_ARGS,
        )

    async def test_deleting_event_art_type(self):
        # artwork_indexer_a_del_event_art_type

        await self.pg_conn.execute(dedent('''
            DELETE FROM event_art_archive.event_art_type
                WHERE id = 1 AND type_id = 1
        '''))

        new_image1_json = self._orig_image1_json | \
            {'front': False, 'types': []}

        await self._event1_reindex_test(
            event_id=1,
            images_json=[new_image1_json],
        )


if __name__ == '__main__':
    unittest.main(verbosity=2)
