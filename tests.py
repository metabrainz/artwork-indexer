import asyncpg
import configparser
import json
import unittest
from aiohttp import ClientSession
from contextlib import asynccontextmanager
from urllib.parse import urlparse
from textwrap import dedent
import indexer
from projects import CAA_PROJECT, EAA_PROJECT


tests_config = configparser.ConfigParser()
tests_config.read('config.tests.ini')


MBS_TEST_URL = tests_config['musicbrainz']['url']
MBS_TEST_NETLOC = urlparse(MBS_TEST_URL).netloc

US_AREA_XML = (
    '<area id="489ce91b-6658-3307-9877-795b68554c98">'
        '<name>United States</name>'
        '<sort-name>United States</sort-name>'
        '<iso-3166-1-code-list>'
            '<iso-3166-1-code>US</iso-3166-1-code>'
        '</iso-3166-1-code-list>'
    '</area>'
)

RELEASE_XML_TEMPLATE = (
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    '<metadata xmlns="http://musicbrainz.org/ns/mmd-2.0#">'
        '<release id="{mbid}">'
            '<title>{title}</title>'
            '<quality>normal</quality>'
            '<artist-credit>'
                '<name-credit>'
                    '<name>{artist_credited_name}</name>'
                    '<artist id="ae859a2d-5754-4e88-9af0-6df263345535">'
                        '<name>{artist_name}</name>'
                        '<sort-name>{artist_sort_name}</sort-name>'
                    '</artist>'
                '</name-credit>'
            '</artist-credit>'
            '{release_event_xml}'
            '{asin_xml}'
            '<cover-art-archive>'
                '<artwork>{has_artwork}</artwork>'
                '<count>{caa_count}</count>'
                '<front>{is_front}</front>'
                '<back>{is_back}</back>'
            '</cover-art-archive>'
        '</release>'
    '</metadata>\n'
)

RELEASE1_MBID = '16ebbc86-670c-4ad3-980b-bfbd1eee4ff4'
RELEASE2_MBID = '2198f7b1-658c-4217-8cae-f63abe0b2391'

RELEASE1_XML_FMT_ARGS = {
    'mbid': RELEASE1_MBID,
    'title': 'ⶵ⮮',
    'artist_name': '🀽',
    'artist_sort_name': '🀽',
    'artist_credited_name': '✺⧳',
    'release_event_xml': (
        '<date>1989-10</date>'
        '<country>US</country>'
        '<release-event-list count="1">'
            '<release-event>'
                '<date>1989-10</date>'
                f'{US_AREA_XML}'
            '</release-event>'
        '</release-event-list>'
    ),
    'asin_xml': '',
    'has_artwork': 'true',
    'caa_count': 1,
    'is_front': 'true',
    'is_back': 'false',
}

RELEASE1_XML = RELEASE_XML_TEMPLATE.format(**RELEASE1_XML_FMT_ARGS)

RELEASE2_XML_FMT_ARGS = {
    'mbid': RELEASE2_MBID,
    'title': 'artless',
    'artist_name': '🀽',
    'artist_sort_name': '🀽',
    'artist_credited_name': '✺⧳',
    'release_event_xml': (
        '<date>1991</date>'
        '<country>US</country>'
        '<release-event-list count="1">'
            '<release-event>'
                '<date>1991</date>'
                f'{US_AREA_XML}'
            '</release-event>'
        '</release-event-list>'
    ),
    'asin_xml': '',
    'has_artwork': 'true',
    'caa_count': 1,
    'is_front': 'false',
    'is_back': 'false',
}

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


class MockResponse():

    def __init__(self, status=200, text=''):
        self._status = status
        self._text = text

    def __await__(self):
        yield

    async def __aenter__(self):
        return self

    async def __exit__(self):
        return self

    async def text(self):
        return self._text


class MockClientSession():

    def __init__(self):
        self.last_requests = []
        self.next_responses = []
        self.session = ClientSession(raise_for_status=True)

    async def __aenter__(self, *args):
        return self

    async def __aexit__(self, *args):
        return self

    def _get_next_response(self):
        if self.next_responses:
            resp = self.next_responses.pop(0)
            if resp._status != 200:
                raise Exception('HTTP %d' % resp._status)
            return resp
        return MockResponse()

    def get(self, url, headers=None):
        self.last_requests.append({
            'method': 'GET',
            'url': url,
            'headers': headers,
            'data': None
        })
        netloc = urlparse(url).netloc
        if netloc == MBS_TEST_NETLOC:
            return self.session.get(url, headers=headers)
        return self._get_next_response()

    def put(self, url, headers=None, data=None):
        self.last_requests.append({
            'method': 'PUT',
            'url': url,
            'headers': headers,
            'data': data.decode('utf-8') if data else None,
        })
        return self._get_next_response()

    def delete(self, url, headers=None):
        self.last_requests.append({
            'method': 'DELETE',
            'url': url,
            'headers': headers,
            'data': None
        })
        return self._get_next_response()

    async def close(self):
        await self.session.close()


def record_items(rec):
    # ignore datetime columns
    for (key, value) in rec.items():
        if key not in ('created', 'last_updated'):
            if key == 'message':
                value = json.loads(value)
            yield (key, value)


def index_event(mbid, **kwargs):
    return {
        'state': 'queued',
        'action': 'index',
        'message': {'gid': mbid},
        'depends_on': None,
        'attempts': 0,
        **kwargs
    }


def release_index_event(mbid, **kwargs):
    return index_event(mbid, entity_type='release', **kwargs)


def event_index_event(mbid, **kwargs):
    return index_event(mbid, entity_type='event', **kwargs)


def index_json_put(project, mbid, images):
    entity_type = project['entity_table']
    image_loc = f"https://{project['domain']}/{entity_type}/{mbid}"

    return {
        'method': 'PUT',
        'url': f'http://mbid-{mbid}.s3.example.com/index.json',
        'headers': {
            'authorization': 'LOW user:pass',
            'content-type': 'application/json; charset=US-ASCII',
            'x-archive-auto-make-bucket': '1',
            'x-archive-keep-old-version': '1',
            'x-archive-meta-collection': project['ia_collection'],
        },
        'data': json.dumps({
            'images': [
                {
                    **image,
                    'image': f"{image_loc}/{image['id']}.jpg",
                    'thumbnails': {
                        '1200': f"{image_loc}/{image['id']}-1200.jpg",
                        '250': f"{image_loc}/{image['id']}-250.jpg",
                        '500': f"{image_loc}/{image['id']}-500.jpg",
                        'large': f"{image_loc}/{image['id']}-500.jpg",
                        'small': f"{image_loc}/{image['id']}-250.jpg",
                    },
                } for image in images
            ],
            f'{entity_type}': f'https://musicbrainz.org/{entity_type}/{mbid}',
        }, sort_keys=True)
    }


def release_index_json_put(mbid, images):
    return index_json_put(CAA_PROJECT, mbid, images)


def event_index_json_put(mbid, images):
    return index_json_put(EAA_PROJECT, mbid, images)


def mb_metadata_xml_get(project, mbid):
    entity_type = project['entity_table']
    ws_inc_params = project['ws_inc_params']
    return {
        'method': 'GET',
        'url': f'{MBS_TEST_URL}/ws/2/{entity_type}/{mbid}?inc={ws_inc_params}',
        'headers': {'mb-set-database': 'TEST_ARTWORK_INDEXER'},
        'data': None,
    }


def release_mb_metadata_xml_get(mbid):
    return mb_metadata_xml_get(CAA_PROJECT, mbid)


def event_mb_metadata_xml_get(mbid):
    return mb_metadata_xml_get(EAA_PROJECT, mbid)


def mb_metadata_xml_put(project, mbid, xml):
    return {
        'method': 'PUT',
        'url': f'http://mbid-{mbid}.s3.example.com/mbid-{mbid}_mb_metadata.xml',
        'headers': {
            'authorization': 'LOW user:pass',
            'content-type': 'application/xml; charset=UTF-8',
            'x-archive-auto-make-bucket': '1',
            'x-archive-meta-collection': project['ia_collection'],
        },
        'data': xml,
    }


def release_mb_metadata_xml_put(mbid, xml):
    return mb_metadata_xml_put(CAA_PROJECT, mbid, xml)


def event_mb_metadata_xml_put(mbid, xml):
    return mb_metadata_xml_put(EAA_PROJECT, mbid, xml)


def image_copy_put(
        project, source_mbid, target_mbid, image_id):
    return {
        'method': 'PUT',
        'url': f'http://mbid-{target_mbid}.s3.example.com/mbid-{target_mbid}-{image_id}.jpg',
        'headers': {
            'authorization': 'LOW user:pass',
            'x-amz-copy-source': f'/mbid-{source_mbid}/mbid-{source_mbid}-{image_id}.jpg',
            'x-archive-auto-make-bucket': '1',
            'x-archive-keep-old-version': '1',
            'x-archive-meta-collection': project['ia_collection'],
            'x-archive-meta-mediatype': 'image',
            'x-archive-meta-noindex': 'true',
        },
        'data': None,
    }


def release_image_copy_put(source_mbid, target_mbid, image_id):
    return image_copy_put(CAA_PROJECT, source_mbid, target_mbid, image_id)


def event_image_copy_put(source_mbid, target_mbid, image_id):
    return image_copy_put(EAA_PROJECT, source_mbid, target_mbid, image_id)


class TestArtArchive(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.last_requests = []
        self.next_responses = []

        self.pg_conn = await asyncpg.connect(**tests_config['database'])
        self.session = MockClientSession()

        @asynccontextmanager
        async def http_client_cls(*args, **kwargs):
            try:
                if self.session.session.closed:
                    self.session = MockClientSession()
                yield self.session
            finally:
                await self.session.close()

        self.http_client_cls = http_client_cls

    async def asyncTearDown(self):
        await self.pg_conn.close()
        await self.session.close()

    async def get_event_queue(self):
        events = await self.pg_conn.fetch(dedent('''
            SELECT * FROM artwork_indexer.event_queue
            WHERE state != 'completed'
            ORDER BY id
        '''))
        return [dict(record_items(x)) for x in events]


class TestCoverArtArchive(TestArtArchive):

    async def asyncSetUp(self):
        await super().asyncSetUp()

        await self.pg_conn.execute(dedent('''
            INSERT INTO area (id, gid, name, type, edits_pending, last_updated, begin_date_year, begin_date_month, begin_date_day, end_date_year, end_date_month, end_date_day, ended, comment)
                VALUES (222, '489ce91b-6658-3307-9877-795b68554c98', 'United States', 1, 0, '2013-06-15 18:06:39.59323+00', NULL, NULL, NULL, NULL, NULL, NULL, '0', '');

            INSERT INTO country_area (area)
                VALUES (222);

            INSERT INTO iso_3166_1 (area, code)
                VALUES (222, 'US');

            INSERT INTO musicbrainz.artist (id, gid, name, sort_name)
                VALUES (1, 'ae859a2d-5754-4e88-9af0-6df263345535', '🀽', '🀽'),
                       (2, '4698a32d-b014-4da6-bdb7-de59fa5179bc', 'O', 'O');

            INSERT INTO musicbrainz.artist_credit (id, gid, name, artist_count)
                VALUES (1, '87d69648-5604-4237-929d-6d2774867811', '✺⧳', 1),
                       (2, '10823f6d-546f-49cc-bc74-0d1095666186', 'O', 1);

            INSERT INTO musicbrainz.artist_credit_name (artist_credit, name, artist, position)
                VALUES (1, '✺⧳', 1, 1),
                       (2, 'O', 2, 1);

            INSERT INTO musicbrainz.release_group (id, gid, name, artist_credit)
                VALUES (1, '9fc47cc7-7a57-4248-b194-75cacadd3646', '⟦⯛', 1);

            INSERT INTO musicbrainz.release (id, gid, name, release_group, artist_credit)
                VALUES (1, '16ebbc86-670c-4ad3-980b-bfbd1eee4ff4', 'ⶵ⮮', 1, 1),
                       (2, '2198f7b1-658c-4217-8cae-f63abe0b2391', 'artless', 1, 1),
                       (3, '41f27dcf-f012-4c91-afc0-0531e196bbda', 'artful', 1, 2);

            INSERT INTO release_country (release, country, date_year, date_month, date_day)
                VALUES (1, 222, 1989, 10, NULL),
                       (2, 222, 1991, NULL, NULL);

            INSERT INTO musicbrainz.editor (id, name, password, ha1, email, email_confirm_date)
                VALUES (10, 'Editor', '{CLEARTEXT}pass', 'b5ba49bbd92eb35ddb35b5acd039440d', 'Editor@example.com', now());

            INSERT INTO musicbrainz.edit (id, editor, type, status, expire_time)
                VALUES (1, 10, 314, 2, now()),
                       (2, 10, 314, 2, now());

            INSERT INTO musicbrainz.edit_data (edit, data)
                VALUES (1, '{}'),
                       (2, '{}');

            INSERT INTO cover_art_archive.cover_art (id, release, mime_type, edit, ordering, comment)
                VALUES (1, 1, 'image/jpeg', 1, 1, '❇'),
                       (2, 3, 'image/png', 2, 1, '?');

            INSERT INTO cover_art_archive.cover_art_type (id, type_id)
                VALUES (1, 1);

            TRUNCATE artwork_indexer.event_queue CASCADE;

            SELECT setval('artwork_indexer.event_queue_id_seq', 1, FALSE);
        '''))

        self._orig_image1_json = {
            'approved': False,
            'back': False,
            'comment': '❇',
            'edit': 1,
            'front': True,
            'id': 1,
            'types': ['Front'],
        }

    async def asyncTearDown(self):
        await self.pg_conn.execute(dedent('''
            TRUNCATE musicbrainz.area CASCADE;
            TRUNCATE musicbrainz.artist CASCADE;
            TRUNCATE musicbrainz.artist_credit CASCADE;
            TRUNCATE musicbrainz.editor CASCADE;
        '''))
        await super().asyncTearDown()

    async def _release_reindex_test(self,
                                    release_mbid=None,
                                    event_id=None,
                                    images_json=None,
                                    xml_fmt_args_base=None,
                                    xml_fmt_args=None):
        self.session.last_requests = []

        self.assertEqual(await self.get_event_queue(), [
            release_index_event(release_mbid, id=event_id),
        ])

        await indexer.indexer(tests_config, 1, max_idle_loops=1, http_client_cls=self.http_client_cls)

        self.assertEqual(self.session.last_requests, [
            release_index_json_put(release_mbid, images_json),
            release_mb_metadata_xml_get(release_mbid),
            release_mb_metadata_xml_put(
                release_mbid,
                RELEASE_XML_TEMPLATE.format(
                    **(xml_fmt_args_base | (xml_fmt_args or {}))
                )
            ),
        ])

    async def _release1_reindex_test(self,
                                     event_id=None,
                                     images_json=None,
                                     xml_fmt_args=None):
        await self._release_reindex_test(
            release_mbid=RELEASE1_MBID,
            event_id=event_id,
            images_json=images_json,
            xml_fmt_args_base=RELEASE1_XML_FMT_ARGS,
            xml_fmt_args=xml_fmt_args
        )

    async def _release2_reindex_test(self,
                                     event_id=None,
                                     images_json=None,
                                     xml_fmt_args=None):
        await self._release_reindex_test(
            release_mbid=RELEASE2_MBID,
            event_id=event_id,
            images_json=images_json,
            xml_fmt_args_base=RELEASE2_XML_FMT_ARGS,
            xml_fmt_args=xml_fmt_args
        )

    async def test_inserting_cover_art(self):
        await self.pg_conn.execute(dedent('''
            INSERT INTO musicbrainz.edit (id, editor, type, status, expire_time)
                VALUES (3, 10, 314, 2, now());

            INSERT INTO musicbrainz.edit_data (edit, data)
                VALUES (3, '{}');

            INSERT INTO cover_art_archive.cover_art (id, release, mime_type, edit, ordering, comment)
                VALUES (3, 1, 'image/jpeg', 3, 2, 'page 1');
        '''))

        await self._release1_reindex_test(
            event_id=1,
            images_json=[
                self._orig_image1_json,
                {
                    'approved': False,
                    'back': False,
                    'comment': 'page 1',
                    'edit': 3,
                    'front': False,
                    'id': 3,
                    'types': [],
                },
            ],
            xml_fmt_args={'caa_count': 2},
        )

    async def test_updating_cover_art(self):
        # a_upd_cover_art_caa

        await self.pg_conn.execute(dedent('''
            UPDATE cover_art_archive.cover_art
                SET ordering = 3, comment = ''
                WHERE id = 1
        '''))

        new_image1_json = self._orig_image1_json | {'comment': ''}

        await self._release1_reindex_test(
            event_id=1,
            images_json=[new_image1_json],
        )

    async def test_deleting_cover_art(self):
        # a_del_cover_art_caa

        await self.pg_conn.execute(dedent('''
            DELETE FROM cover_art_archive.cover_art
                WHERE id = 1
        '''))

        self.session.last_requests = []

        self.assertEqual(await self.get_event_queue(), [
            {
                'id': 1,
                'state': 'queued',
                'entity_type': 'release',
                'action': 'delete_image',
                'message': {
                    'artwork_id': 1,
                    'gid': RELEASE1_MBID,
                    'suffix': 'jpg',
                },
                'depends_on': None,
                'attempts': 0,
            },
            release_index_event(RELEASE1_MBID, id=2, depends_on=[1]),
        ])

        await indexer.indexer(tests_config, 1, max_idle_loops=1, http_client_cls=self.http_client_cls)

        self.assertEqual(self.session.last_requests, [
            {
                'method': 'DELETE',
                'url': f'http://mbid-{RELEASE1_MBID}.s3.example.com/mbid-{RELEASE1_MBID}-1.jpg',
                'data': None,
                'headers': {
                    'authorization': 'LOW user:pass',
                    'x-archive-cascade-delete': '1',
                    'x-archive-keep-old-version': '1',
                },
            },
            release_index_json_put(RELEASE1_MBID, []),
            release_mb_metadata_xml_get(RELEASE1_MBID),
            release_mb_metadata_xml_put(
                RELEASE1_MBID,
                RELEASE_XML_TEMPLATE.format(
                    **(
                        RELEASE1_XML_FMT_ARGS |
                        {
                            'has_artwork': 'false',
                            'is_front': 'false',
                            'is_back': 'false',
                            'caa_count': '0'
                        }
                    )
                )
            ),
        ])

    async def test_deleting_release(self):
        # a_del_release_caa

        # Queue an index event (via a_upd_cover_art_caa). We're checking that
        # it's replaced by the following release deletion event.
        await self.pg_conn.execute(dedent('''
            UPDATE cover_art_archive.cover_art
                SET ordering = 3, comment = ''
                WHERE id = 1
        '''))

        new_image1_json = self._orig_image1_json | {'comment': ''}

        self.assertEqual(await self.get_event_queue(), [
            release_index_event(RELEASE1_MBID, id=1),
        ])

        # This simulates a merge, where the cover art is first copied to
        # another release, and the original release is deleted.
        await self.pg_conn.execute(dedent('''
            UPDATE cover_art_archive.cover_art
                SET release = 2
                WHERE id = 1;

            UPDATE release_meta
                SET cover_art_presence = 'present'
                WHERE id = 2;

            DELETE FROM release_country
            WHERE (release, country) IN (
                SELECT release, country
                FROM (
                    SELECT release, country,
                        (row_number() OVER (
                            PARTITION BY country
                            ORDER BY date_year IS NOT NULL DESC,
                                     date_month IS NOT NULL DESC,
                                     date_day IS NOT NULL DESC,
                                     release = 2 DESC)
                        ) > 1 AS remove
                    FROM release_country
                    WHERE release IN (1, 2)
                ) a
                WHERE remove
            );

            UPDATE release_country SET release = 2 WHERE release = 1;
            DELETE FROM release WHERE id = 1;
        '''))

        self.assertEqual(await self.get_event_queue(), [
            {
                'id': 5,
                'state': 'queued',
                'entity_type': 'release',
                'action': 'copy_image',
                'message': {
                    'artwork_id': 1,
                    'new_gid': RELEASE2_MBID,
                    'old_gid': RELEASE1_MBID,
                    'suffix': 'jpg',
                },
                'depends_on': None,
                'attempts': 0,
            },
            {
                'id': 6,
                'state': 'queued',
                'entity_type': 'release',
                'action': 'delete_image',
                'message': {
                    'artwork_id': 1,
                    'gid': RELEASE1_MBID,
                    'suffix': 'jpg',
                },
                'depends_on': [5],
                'attempts': 0,
            },
            release_index_event(RELEASE2_MBID, id=8, depends_on=[6]),
            {
                'id': 11,
                'state': 'queued',
                'entity_type': 'release',
                'action': 'deindex',
                'message': {'gid': RELEASE1_MBID},
                'depends_on': None,
                'attempts': 0,
            },
        ])

        # Make the copy fail. This should halt processing of all dependant events
        # (delete_image, index).
        print('note, the following test is expected to log an HTTP 400 error')
        self.session.next_responses.append(MockResponse(status=400))

        await self.pg_conn.execute(dedent('''
            UPDATE artwork_indexer.event_queue
            SET attempts = 4, last_updated = (now() - interval '1 day')
            WHERE action = 'copy_image'
        '''))

        self.session.last_requests = []
        await indexer.indexer(tests_config, 1, max_idle_loops=1, http_client_cls=self.http_client_cls)

        self.assertEqual(await self.get_event_queue(), [
            {
                'id': 5,
                'state': 'failed',
                'entity_type': 'release',
                'action': 'copy_image',
                'message': {
                    'artwork_id': 1,
                    'new_gid': RELEASE2_MBID,
                    'old_gid': RELEASE1_MBID,
                    'suffix': 'jpg',
                },
                'depends_on': None,
                'attempts': 5,
            },
            {
                'id': 6,
                'state': 'queued',
                'entity_type': 'release',
                'action': 'delete_image',
                'message': {
                    'artwork_id': 1,
                    'gid': RELEASE1_MBID,
                    'suffix': 'jpg',
                },
                'depends_on': [5],
                'attempts': 0,
            },
            release_index_event(RELEASE2_MBID, id=8, depends_on=[6]),
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
            release_image_copy_put(RELEASE1_MBID, RELEASE2_MBID, 1),
            {
                'method': 'DELETE',
                'url': f'http://mbid-{RELEASE1_MBID}.s3.example.com/index.json',
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

        await indexer.indexer(tests_config, 1, max_idle_loops=1, http_client_cls=self.http_client_cls)

        self.assertEqual(self.session.last_requests, [
            release_image_copy_put(RELEASE1_MBID, RELEASE2_MBID, 1),
            {
                'method': 'DELETE',
                'url': f'http://mbid-{RELEASE1_MBID}.s3.example.com/mbid-{RELEASE1_MBID}-1.jpg',
                'data': None,
                'headers': {
                    'authorization': 'LOW user:pass',
                    'x-archive-cascade-delete': '1',
                    'x-archive-keep-old-version': '1',
                },
            },
            release_index_json_put(RELEASE2_MBID, [new_image1_json]),
            release_mb_metadata_xml_get(RELEASE2_MBID),
            release_mb_metadata_xml_put(
                RELEASE2_MBID,
                RELEASE_XML_TEMPLATE.format(
                   **(
                        RELEASE2_XML_FMT_ARGS |
                        {
                            'is_front': 'true',
                            'release_event_xml': (
                                '<date>1989-10</date>'
                                '<country>US</country>'
                                '<release-event-list count="1">'
                                    '<release-event>'
                                        '<date>1989-10</date>'
                                        f'{US_AREA_XML}'
                                    '</release-event>'
                                '</release-event-list>'
                            )
                        }
                    )
                )
            ),
        ])

    async def test_inserting_cover_art_type(self):
        # a_ins_cover_art_type_caa

        await self.pg_conn.execute(dedent('''
            INSERT INTO cover_art_archive.cover_art_type (id, type_id)
                VALUES (1, 2);
        '''))

        new_image1_json = (
            self._orig_image1_json |
            {'back': True, 'types': ['Front', 'Back']}
        )

        await self._release1_reindex_test(
            event_id=1,
            images_json=[new_image1_json],
            xml_fmt_args={'is_back': 'true'},
        )

    async def test_deleting_cover_art_type(self):
        # a_del_cover_art_type_caa

        await self.pg_conn.execute(dedent('''
            DELETE FROM cover_art_archive.cover_art_type
                WHERE id = 1 AND type_id = 1
        '''))

        new_image1_json = self._orig_image1_json | {'front': False, 'types': []}

        await self._release1_reindex_test(
            event_id=1,
            images_json=[new_image1_json],
            xml_fmt_args={'is_front': 'false'},
        )

    async def test_updating_artist(self):
        # a_upd_artist_caa

        await self.pg_conn.execute(dedent('''
            UPDATE artist SET name = 'foo', sort_name = 'bar' WHERE id = 1;
        '''))

        await self._release1_reindex_test(
            event_id=1,
            images_json=[self._orig_image1_json],
            xml_fmt_args={
                'artist_name': 'foo',
                'artist_sort_name': 'bar',
            },
        )

    async def test_updating_release(self):
        # a_upd_release_caa

        await self.pg_conn.execute(dedent('''
            UPDATE release SET name = 'updated name1' WHERE id = 1;

            -- Should not produce any update, as there is no cover art
            -- associated with this release.
            UPDATE release SET name = 'updated name2' WHERE id = 2;
        '''))

        await self._release1_reindex_test(
            event_id=1,
            images_json=[self._orig_image1_json],
            xml_fmt_args={
                'title': 'updated name1',
            },
        )

    async def test_updating_release_meta(self):
        # a_upd_release_meta_caa

        await self.pg_conn.execute(dedent('''
            UPDATE release_meta SET amazon_asin = 'FOOBAR123' WHERE id = 1;

            -- Should not produce any update, as there is no cover art
            -- associated with this release.
            UPDATE release_meta SET amazon_asin = 'FOOBAR456' WHERE id = 2;
        '''))

        await self._release1_reindex_test(
            event_id=1,
            images_json=[self._orig_image1_json],
            xml_fmt_args={
                'asin_xml': '<asin>FOOBAR123</asin>',
            },
        )

    async def test_inserting_first_release_date(self):
        # a_ins_release_first_release_date_caa

        await self.pg_conn.execute(dedent('''
            INSERT INTO release_unknown_country VALUES (1, 1980, 1, 1);

            -- Should not produce any update, as there is no cover art
            -- associated with this release.
            INSERT INTO release_unknown_country VALUES (2, 1970, 1, 1);
        '''))

        await self._release1_reindex_test(
            event_id=1,
            images_json=[self._orig_image1_json],
            xml_fmt_args={
                'release_event_xml': (
                    '<date>1980-01-01</date>'
                    '<release-event-list count="2">'
                        '<release-event>'
                            '<date>1980-01-01</date>'
                        '</release-event>'
                        '<release-event>'
                            '<date>1989-10</date>'
                            f'{US_AREA_XML}'
                        '</release-event>'
                    '</release-event-list>'
                ),
            },
        )

    async def test_deleting_first_release_date(self):
        # a_del_release_first_release_date_caa

        await self.pg_conn.execute(dedent('''
            DELETE FROM release_country WHERE release = 1;

            -- Should not produce any update, as there is no cover art
            -- associated with this release.
            DELETE FROM release_country WHERE release = 2;
        '''))

        await self._release1_reindex_test(
            event_id=1,
            images_json=[self._orig_image1_json],
            xml_fmt_args={
                'release_event_xml': ''
            },
        )

    async def test_duplicate_updates(self):
        await self.pg_conn.execute(dedent('''
            UPDATE musicbrainz.release SET name = 'update' WHERE id = 1;
            UPDATE cover_art_archive.cover_art SET comment = 'a' WHERE id = 1;
            UPDATE cover_art_archive.cover_art SET comment = 'b' WHERE id = 1;
        '''))

        # Test that duplicate index events are not inserted.
        self.assertEqual(await self.get_event_queue(), [
            release_index_event(RELEASE1_MBID, id=1),
        ])

    async def test_cleanup(self):
        await self.pg_conn.execute(dedent('''
            UPDATE release SET name = 'updated name1' WHERE id = 1;
        '''))

        await indexer.indexer(tests_config, 1, max_idle_loops=1, http_client_cls=self.http_client_cls)

        event_count = await self.pg_conn.fetchval(dedent('''
            SELECT count(*) FROM artwork_indexer.event_queue
        '''))
        self.assertEqual(event_count, 1)

        await self.pg_conn.execute(dedent('''
            UPDATE artwork_indexer.event_queue
            SET created = (created - interval '90 days');
        '''))

        await indexer.indexer(tests_config, 2, max_idle_loops=2, http_client_cls=self.http_client_cls)

        event_count = await self.pg_conn.fetchval(dedent('''
            SELECT count(*) FROM artwork_indexer.event_queue
        '''))
        self.assertEqual(event_count, 0)


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
            INSERT INTO musicbrainz.event (id, gid, name, begin_date_year, end_date_year, time, type)
                VALUES (1, 'e2aad65a-12e0-44ec-b693-94d225154e90', 'live at the place 1', 1990, 1990, '20:00', 1),
                       (2, 'a0f19ff3-e140-417f-81c6-2a7466eeea0a', 'live at the place 2', 1991, 1991, '21:00', 1);

            INSERT INTO musicbrainz.editor (id, name, password, ha1, email, email_confirm_date)
                VALUES (10, 'Editor', '{CLEARTEXT}pass', 'b5ba49bbd92eb35ddb35b5acd039440d', 'Editor@example.com', now());

            INSERT INTO musicbrainz.edit (id, editor, type, status, expire_time)
                VALUES (1, 10, 158, 2, now()),
                       (2, 10, 158, 2, now());

            INSERT INTO musicbrainz.edit_data (edit, data)
                VALUES (1, '{}'),
                       (2, '{}');

            INSERT INTO event_art_archive.event_art (id, event, mime_type, edit, ordering, comment)
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

        await indexer.indexer(tests_config, 1, max_idle_loops=1, http_client_cls=self.http_client_cls)

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
        # a_ins_event_art_eaa

        await self.pg_conn.execute(dedent('''
            INSERT INTO musicbrainz.edit (id, editor, type, status, expire_time)
                VALUES (3, 10, 158, 2, now());

            INSERT INTO musicbrainz.edit_data (edit, data)
                VALUES (3, '{}');

            INSERT INTO event_art_archive.event_art (id, event, mime_type, edit, ordering, comment)
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
        # a_upd_event_art_eaa

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
        # a_del_event_art_caa

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

        await indexer.indexer(tests_config, 1, max_idle_loops=1, http_client_cls=self.http_client_cls)

        self.assertEqual(self.session.last_requests, [
            {
                'method': 'DELETE',
                'url': f'http://mbid-{EVENT1_MBID}.s3.example.com/mbid-{EVENT1_MBID}-1.jpg',
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
        # a_del_event_eaa

        # Queue an index event (via a_upd_event_art_eaa). We're checking that
        # it's replaced by the following event (entity) deletion event.
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

        # Make the copy fail. This should halt processing of all dependant events
        # (delete_image, index).
        print('note, the following test is expected to log an HTTP 400 error')
        self.session.next_responses.append(MockResponse(status=400))

        await self.pg_conn.execute(dedent('''
            UPDATE artwork_indexer.event_queue
            SET attempts = 4, last_updated = (now() - interval '1 day')
            WHERE action = 'copy_image'
        '''))

        self.session.last_requests = []
        await indexer.indexer(tests_config, 1, max_idle_loops=1, http_client_cls=self.http_client_cls)

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

        await indexer.indexer(tests_config, 1, max_idle_loops=1, http_client_cls=self.http_client_cls)

        self.assertEqual(self.session.last_requests, [
            event_image_copy_put(EVENT1_MBID, EVENT2_MBID, 1),
            {
                'method': 'DELETE',
                'url': f'http://mbid-{EVENT1_MBID}.s3.example.com/mbid-{EVENT1_MBID}-1.jpg',
                'data': None,
                'headers': {
                    'authorization': 'LOW user:pass',
                    'x-archive-cascade-delete': '1',
                    'x-archive-keep-old-version': '1',
                },
            },
            event_index_json_put(EVENT2_MBID, [self._orig_image2_json, new_image1_json]),
            event_mb_metadata_xml_get(EVENT2_MBID),
            event_mb_metadata_xml_put(EVENT2_MBID, EVENT2_XML),
        ])

    async def test_inserting_event_art_type(self):
        # a_ins_event_art_type_eaa

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
        # a_del_event_art_type_eaa

        await self.pg_conn.execute(dedent('''
            DELETE FROM event_art_archive.event_art_type
                WHERE id = 1 AND type_id = 1
        '''))

        new_image1_json = self._orig_image1_json | {'front': False, 'types': []}

        await self._event1_reindex_test(
            event_id=1,
            images_json=[new_image1_json],
        )


if __name__ == '__main__':
    unittest.main(verbosity=2)
