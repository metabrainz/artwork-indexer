import asyncio
import asyncpg
import configparser
import datetime
import json
import unittest
from aiohttp import ClientSession
from urllib.parse import urlparse
from unittest.mock import patch, MagicMock
from textwrap import dedent
import indexer
from projects import CAA_PROJECT, EAA_PROJECT


tests_config = configparser.ConfigParser()
tests_config.read('config.tests.ini')


MBS_TEST_URL = tests_config['musicbrainz']['url']
MBS_TEST_NETLOC = urlparse(MBS_TEST_URL).netloc
NEXT_RESPONSES = []
LAST_REQUESTS = []


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


def get_next_response():
    if NEXT_RESPONSES:
        resp = NEXT_RESPONSES.pop(0)
        if resp._status != 200:
            raise Exception('HTTP %d' % resp._status)
        return resp
    return MockResponse()


class MockClientSession():

    session = None

    def __init__(self, *args, **kwargs):
        if MockClientSession.session is None:
            MockClientSession.session = ClientSession(*args, **kwargs)

    async def __aenter__(self, *args):
        return self

    async def __aexit__(self, *args):
        return self

    def get(self, url, headers=None):
        LAST_REQUESTS.append({
            'method': 'GET',
            'url': url,
            'headers': headers,
            'data': None
        })
        netloc = urlparse(url).netloc
        if netloc == MBS_TEST_NETLOC:
            return self.session.get(url, headers=headers)
        return get_next_response()

    def put(self, url, headers=None, data=None):
        LAST_REQUESTS.append({
            'method': 'PUT',
            'url': url,
            'headers': headers,
            'data': data.decode('utf-8') if data else None,
        })
        return get_next_response()

    def delete(self, url, headers=None):
        LAST_REQUESTS.append({
            'method': 'DELETE',
            'url': url,
            'headers': headers,
            'data': None
        })
        return get_next_response()


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


def copy_image_event(mbid, **kwargs):
    return {
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
        **kwargs
    }


def release_copy_image_event(mbid, **kwargs):
    return copy_image_event(mbid, entity_type='release', **kwargs)


def event_copy_image_event(mbid, **kwargs):
    return copy_image_event(mbid, entity_type='event', **kwargs)


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


class TestCoverArtArchive(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.pg_conn = await asyncpg.connect(**tests_config['database'])

    async def asyncTearDown(self):
        await self.pg_conn.close()

    def _tearDownAsyncioLoop(self):
        if MockClientSession.session:
            self._asyncioTestLoop.run_until_complete(MockClientSession.session.close())
        super()._tearDownAsyncioLoop()

    @patch('aiohttp.ClientSession', new=MockClientSession)
    async def test_triggers(self):
        global LAST_REQUESTS
        global NEXT_RESPONSES

        async def get_event_queue():
            events = await self.pg_conn.fetch(dedent('''
                SELECT * FROM artwork_indexer.event_queue
                WHERE state != 'completed'
                ORDER BY id
            '''))
            return [dict(record_items(x)) for x in events]

        RELEASE_XML = (
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
        RELEASE1_XML = RELEASE_XML.format(
            mbid=RELEASE1_MBID,
            title='ⶵ⮮',
            artist_name='🀽',
            artist_sort_name='🀽',
            artist_credited_name='✺⧳',
            release_event_xml='',
            asin_xml='',
            has_artwork='true',
            caa_count='{caa_count}',
            is_front='false',
            is_back='false',
        )

        # a_ins_cover_art_caa

        await self.pg_conn.execute(dedent('''
            INSERT INTO musicbrainz.artist (id, gid, name, sort_name)
                VALUES (1, 'ae859a2d-5754-4e88-9af0-6df263345535', '🀽', '🀽');

            INSERT INTO musicbrainz.artist_credit (id, gid, name, artist_count)
                VALUES (1, '87d69648-5604-4237-929d-6d2774867811', '✺⧳', 1);

            INSERT INTO musicbrainz.artist_credit_name (artist_credit, name, artist, position)
                VALUES (1, '✺⧳', 1, 1);

            INSERT INTO musicbrainz.release_group (id, gid, name, artist_credit)
                VALUES (1, '9fc47cc7-7a57-4248-b194-75cacadd3646', '⟦⯛', 1);

            INSERT INTO musicbrainz.release (id, gid, name, release_group, artist_credit)
                VALUES (1, '16ebbc86-670c-4ad3-980b-bfbd1eee4ff4', 'ⶵ⮮', 1, 1);

            INSERT INTO musicbrainz.editor (id, name, password, ha1, email, email_confirm_date)
                VALUES (10, 'Editor', '{CLEARTEXT}pass', 'b5ba49bbd92eb35ddb35b5acd039440d', 'Editor@example.com', now());

            INSERT INTO musicbrainz.edit (id, editor, type, status, expire_time)
                VALUES (1, 10, 314, 2, now());

            INSERT INTO musicbrainz.edit_data (edit, data)
                VALUES (1, '{}');

            INSERT INTO cover_art_archive.cover_art (id, release, mime_type, edit, ordering, comment)
                VALUES (1, 1, 'image/jpeg', 1, 1, '❇');
        '''))

        self.assertEqual(await get_event_queue(), [
            release_index_event(RELEASE1_MBID, id=1),
        ])

        await indexer.indexer(tests_config, 1, max_idle_loops=1)

        IMAGE1_JSON = {
            'approved': False,
            'back': False,
            'comment': '❇',
            'edit': 1,
            'front': False,
            'id': 1,
            'types': [],
        }

        self.assertEqual(LAST_REQUESTS, [
            release_index_json_put(RELEASE1_MBID, [IMAGE1_JSON]),
            release_mb_metadata_xml_get(RELEASE1_MBID),
            release_mb_metadata_xml_put(
                RELEASE1_MBID,
                RELEASE1_XML.format(caa_count=1),
            ),
        ])

        # a_upd_cover_art_caa
        # a_del_release_caa

        await self.pg_conn.execute(dedent('''
            UPDATE cover_art_archive.cover_art
                SET ordering = 2, comment = ''
                WHERE id = 1
        '''))

        IMAGE1_JSON['comment'] = ''

        self.assertEqual(await get_event_queue(), [
            release_index_event(RELEASE1_MBID, id=2),
        ])

        RELEASE2_MBID = '2198f7b1-658c-4217-8cae-f63abe0b2391'

        await self.pg_conn.execute(dedent('''
            INSERT INTO musicbrainz.release (id, gid, name, release_group, artist_credit)
                VALUES (2, '2198f7b1-658c-4217-8cae-f63abe0b2391', 'new release', 1, 1);

            UPDATE cover_art_archive.cover_art
                SET release = 2
                WHERE id = 1;

            UPDATE release_meta
                SET cover_art_presence = 'present'
                WHERE id = 2;

            DELETE FROM release WHERE id = 1;
        '''))

        self.assertEqual(await get_event_queue(), [
            {
                'id': 6,
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
                'id': 7,
                'state': 'queued',
                'entity_type': 'release',
                'action': 'delete_image',
                'message': {
                    'artwork_id': 1,
                    'gid': RELEASE1_MBID,
                    'suffix': 'jpg',
                },
                'depends_on': [6],
                'attempts': 0,
            },
            release_index_event(RELEASE2_MBID, id=9, depends_on=[7]),
            {
                'id': 10,
                'state': 'queued',
                'entity_type': 'release',
                'action': 'deindex',
                'message': {'gid': RELEASE1_MBID},
                'depends_on': None,
                'attempts': 0,
            },
        ])

        # Make the copy fail.
        NEXT_RESPONSES.append(MockResponse(status=400))

        await self.pg_conn.execute(dedent('''
            UPDATE artwork_indexer.event_queue
            SET attempts = 4, last_updated = (now() - interval '1 day')
            WHERE action = 'copy_image'
        '''))

        LAST_REQUESTS = []
        await indexer.indexer(tests_config, 1, max_idle_loops=1)

        self.assertEqual(await get_event_queue(), [
            {
                'id': 6,
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
                'id': 7,
                'state': 'queued',
                'entity_type': 'release',
                'action': 'delete_image',
                'message': {
                    'artwork_id': 1,
                    'gid': RELEASE1_MBID,
                    'suffix': 'jpg',
                },
                'depends_on': [6],
                'attempts': 0,
            },
            release_index_event(RELEASE2_MBID, id=9, depends_on=[7]),
        ])

        self.assertEqual(
            await self.pg_conn.fetchval(dedent('''
                SELECT failure_reason
                FROM artwork_indexer.event_failure_reason
                WHERE event = 6
            ''')),
            'HTTP 400',
        )

        self.assertEqual(LAST_REQUESTS, [
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

        # Revert the artificial failure we created.
        await self.pg_conn.execute(dedent('''
            UPDATE artwork_indexer.event_queue
            SET attempts = 0, state = 'queued'
            WHERE action = 'copy_image'
        '''))

        LAST_REQUESTS = []
        RELEASE2_XML_FMT_ARGS = {
            'mbid': RELEASE2_MBID,
            'title': 'new release',
            'artist_name': '🀽',
            'artist_sort_name': '🀽',
            'artist_credited_name': '✺⧳',
            'release_event_xml': '',
            'asin_xml': '',
            'has_artwork': 'true',
            'caa_count': 1,
            'is_front': 'false',
            'is_back': 'false',
        }
        RELEASE2_XML = RELEASE_XML.format(**RELEASE2_XML_FMT_ARGS)

        await indexer.indexer(tests_config, 1, max_idle_loops=1)

        self.assertEqual(LAST_REQUESTS, [
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
            release_index_json_put(RELEASE2_MBID, [IMAGE1_JSON]),
            release_mb_metadata_xml_get(RELEASE2_MBID),
            release_mb_metadata_xml_put(RELEASE2_MBID, RELEASE2_XML),
        ])

        async def release2_reindex_test(
                event_id=None,
                sql_query='',
                image_json_props=None,
                xml_fmt_args=None):
            await self.pg_conn.execute(sql_query)

            global LAST_REQUESTS
            LAST_REQUESTS = []

            if image_json_props:
                IMAGE1_JSON.update(image_json_props)

            if xml_fmt_args:
                RELEASE2_XML_FMT_ARGS.update(xml_fmt_args)
                RELEASE2_XML = RELEASE_XML.format(**RELEASE2_XML_FMT_ARGS)

            self.assertEqual(await get_event_queue(), [
                release_index_event(RELEASE2_MBID, id=event_id),
            ])

            await indexer.indexer(tests_config, 1, max_idle_loops=1)

            self.assertEqual(LAST_REQUESTS, [
                release_index_json_put(RELEASE2_MBID, [IMAGE1_JSON]),
                release_mb_metadata_xml_get(RELEASE2_MBID),
                release_mb_metadata_xml_put(RELEASE2_MBID, RELEASE2_XML),
            ])

        # a_ins_cover_art_type_caa

        await release2_reindex_test(
            event_id=11,
            sql_query=dedent('''
                INSERT INTO cover_art_archive.cover_art_type (id, type_id)
                    VALUES (1, 1), (1, 2);
            '''),
            image_json_props={'front': True, 'back': True, 'types': ['Front', 'Back']},
            xml_fmt_args={'is_front': 'true', 'is_back': 'true'},
        )

        # a_del_cover_art_type_caa

        await release2_reindex_test(
            event_id=13,
            sql_query=dedent('''
                DELETE FROM cover_art_archive.cover_art_type
                    WHERE id = 1 AND type_id = 2
            '''),
            image_json_props={'back': False, 'types': ['Front']},
            xml_fmt_args={'is_back': 'false'},
        )

        # a_upd_artist_caa

        await release2_reindex_test(
            event_id=14,
            sql_query=dedent('''
                UPDATE artist
                    SET name = 'foo', sort_name = 'bar'
                    WHERE id = 1;
            '''),
            xml_fmt_args={'artist_name': 'foo', 'artist_sort_name': 'bar'},
        )

        # a_upd_release_caa

        await release2_reindex_test(
            event_id=15,
            sql_query=dedent('''
                UPDATE release
                    SET name = 'updated name'
                    WHERE id = 2
            '''),
            xml_fmt_args={'title': 'updated name'},
        )

        # a_upd_release_meta_caa

        await release2_reindex_test(
            event_id=16,
            sql_query=dedent('''
                UPDATE release_meta
                    SET amazon_asin = 'FOOBAR123'
                    WHERE id = 2
            '''),
            xml_fmt_args={'asin_xml': '<asin>FOOBAR123</asin>'},
        )

        # a_ins_release_first_release_date_caa

        await release2_reindex_test(
            event_id=17,
            sql_query=dedent('''
                INSERT INTO release_unknown_country
                    VALUES (2, 1990, 1, 1);
            '''),
            xml_fmt_args={
                'release_event_xml': (
                    '<date>1990-01-01</date>'
                    '<release-event-list count="1">'
                        '<release-event>'
                            '<date>1990-01-01</date>'
                        '</release-event>'
                    '</release-event-list>'
                ),
            },
        )

        # a_del_release_first_release_date_caa

        await release2_reindex_test(
            event_id=18,
            sql_query=dedent('''
                DELETE FROM release_unknown_country WHERE release = 2;
            '''),
            xml_fmt_args={'release_event_xml': ''},
        )

        # a_del_cover_art_caa

        await self.pg_conn.execute(dedent('''
            DELETE FROM cover_art_archive.cover_art
                WHERE id = 1
        '''))

        LAST_REQUESTS = []
        RELEASE2_XML_FMT_ARGS['has_artwork'] = 'false'
        RELEASE2_XML_FMT_ARGS['caa_count'] = '0'
        RELEASE2_XML_FMT_ARGS['is_front'] = 'false'
        RELEASE2_XML = RELEASE_XML.format(**RELEASE2_XML_FMT_ARGS)

        self.assertEqual(await get_event_queue(), [
            {
                'id': 19,
                'state': 'queued',
                'entity_type': 'release',
                'action': 'delete_image',
                'message': {
                    'artwork_id': 1,
                    'gid': RELEASE2_MBID,
                    'suffix': 'jpg',
                },
                'depends_on': None,
                'attempts': 0,
            },
            release_index_event(RELEASE2_MBID, id=20, depends_on=[19]),
        ])

        await indexer.indexer(tests_config, 1, max_idle_loops=1)

        self.assertEqual(LAST_REQUESTS, [
            {
                'method': 'DELETE',
                'url': f'http://mbid-{RELEASE2_MBID}.s3.example.com/mbid-{RELEASE2_MBID}-1.jpg',
                'data': None,
                'headers': {
                    'authorization': 'LOW user:pass',
                    'x-archive-cascade-delete': '1',
                    'x-archive-keep-old-version': '1',
                },
            },
            release_index_json_put(RELEASE2_MBID, []),
            release_mb_metadata_xml_get(RELEASE2_MBID),
            release_mb_metadata_xml_put(RELEASE2_MBID, RELEASE2_XML),
        ])

        # Event Art Archive

        EVENT_XML = (
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

        await self.pg_conn.execute(dedent('''
            INSERT INTO musicbrainz.event (id, gid, name, begin_date_year, end_date_year, time, type)
                VALUES (1, 'e2aad65a-12e0-44ec-b693-94d225154e90', 'live at the place', 1990, 1990, '20:00', 1);

            INSERT INTO musicbrainz.edit (id, editor, type, status, expire_time)
                -- FIXME: there's no $EDIT_EVENT_ADD_EVENT_ART in MB yet, so we're
                -- reusing 314.
                VALUES (2, 10, 314, 2, now());

            INSERT INTO musicbrainz.edit_data (edit, data)
                VALUES (2, '{}');

            INSERT INTO event_art_archive.event_art (id, event, mime_type, edit, ordering, comment)
                VALUES (1, 1, 'image/jpeg', 2, 1, 'hello');
        '''))

        EVENT1_MBID = 'e2aad65a-12e0-44ec-b693-94d225154e90'

        EVENT1_XML = EVENT_XML.format(
            mbid=EVENT1_MBID,
            name='live at the place',
            type_name='Concert',
            type_id='ef55e8d7-3d00-394a-8012-f5506a29ff0b',
            begin='1990',
            end='1990',
            time='20:00',
        )

        IMAGE1_JSON = {
            'approved': False,
            'comment': 'hello',
            'edit': 2,
            'front': False,
            'id': 1,
            'types': [],
        }

        self.assertEqual(await get_event_queue(), [
            event_index_event(EVENT1_MBID, id=21),
        ])

        LAST_REQUESTS = []
        await indexer.indexer(tests_config, 1, max_idle_loops=1)

        self.assertEqual(LAST_REQUESTS, [
            event_index_json_put(EVENT1_MBID, [IMAGE1_JSON]),
            event_mb_metadata_xml_get(EVENT1_MBID),
            event_mb_metadata_xml_put(EVENT1_MBID, EVENT1_XML),
        ])


if __name__ == '__main__':
    unittest.main(verbosity=2)
