import os.path
import unittest
from textwrap import dedent
import indexer
from projects import CAA_PROJECT
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


def release_index_event(mbid, **kwargs):
    return index_event(mbid, entity_type='release', **kwargs)


def release_index_json_put(mbid, images):
    return index_json_put(CAA_PROJECT, mbid, images)


def release_mb_metadata_xml_get(mbid):
    return mb_metadata_xml_get(CAA_PROJECT, mbid)


def release_mb_metadata_xml_put(mbid, xml):
    return mb_metadata_xml_put(CAA_PROJECT, mbid, xml)


def release_image_copy_put(source_mbid, target_mbid, image_id):
    return image_copy_put(CAA_PROJECT, source_mbid, target_mbid, image_id)


class TestCoverArtArchive(TestArtArchive):

    def setUp(self):
        super().setUp()

        with open(
            os.path.join(os.path.dirname(__file__), 'caa_setup.sql'),
            'r'
        ) as fp:
            self.pg_conn.execute(fp.read())

        self._orig_image1_json = {
            'approved': False,
            'back': False,
            'comment': '❇',
            'edit': 1,
            'front': True,
            'id': 1,
            'types': ['Front'],
        }

    def tearDown(self):
        with open(
            os.path.join(os.path.dirname(__file__), 'caa_teardown.sql'),
            'r'
        ) as fp:
            self.pg_conn.execute(fp.read())
        super().tearDown()

    def _release_reindex_test(self,
                              release_mbid=None,
                              event_id=None,
                              images_json=None,
                              xml_fmt_args_base=None,
                              xml_fmt_args=None):
        self.assertEqual(self.get_event_queue(), [
            release_index_event(release_mbid, id=event_id),
        ])

        xml = RELEASE_XML_TEMPLATE.format(
            **(xml_fmt_args_base | (xml_fmt_args or {}))
        )

        self.session.last_requests = []
        self.session.next_responses = [
            MockResponse(),
            MockResponse(status=200, content=xml),
            MockResponse(status=200, content=xml),
        ]

        indexer.indexer(tests_config, 1,
                        max_idle_loops=1,
                        http_client_cls=self.http_client_cls)

        self.assertEqual(self.session.last_requests, [
            release_index_json_put(release_mbid, images_json),
            release_mb_metadata_xml_get(release_mbid),
            release_mb_metadata_xml_put(release_mbid, xml),
        ])

    def _release1_reindex_test(self,
                               event_id=None,
                               images_json=None,
                               xml_fmt_args=None):
        self._release_reindex_test(
            release_mbid=RELEASE1_MBID,
            event_id=event_id,
            images_json=images_json,
            xml_fmt_args_base=RELEASE1_XML_FMT_ARGS,
            xml_fmt_args=xml_fmt_args
        )

    def _release2_reindex_test(self,
                               event_id=None,
                               images_json=None,
                               xml_fmt_args=None):
        self._release_reindex_test(
            release_mbid=RELEASE2_MBID,
            event_id=event_id,
            images_json=images_json,
            xml_fmt_args_base=RELEASE2_XML_FMT_ARGS,
            xml_fmt_args=xml_fmt_args
        )

    def test_inserting_cover_art(self):
        self.pg_conn.execute(dedent('''
            INSERT INTO musicbrainz.edit
                    (id, editor, type, status, expire_time)
                VALUES (3, 10, 314, 2, now());

            INSERT INTO musicbrainz.edit_data (edit, data)
                VALUES (3, '{}');

            INSERT INTO cover_art_archive.cover_art
                    (id, release, mime_type, edit, ordering, comment)
                VALUES (3, 1, 'image/jpeg', 3, 2, 'page 1');
        '''))

        self._release1_reindex_test(
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

    def test_updating_cover_art(self):
        # artwork_indexer_a_upd_cover_art

        self.pg_conn.execute(dedent('''
            UPDATE cover_art_archive.cover_art
                SET ordering = 3, comment = ''
                WHERE id = 1
        '''))

        new_image1_json = self._orig_image1_json | {'comment': ''}

        self._release1_reindex_test(
            event_id=1,
            images_json=[new_image1_json],
        )

    def test_deleting_cover_art(self):
        # artwork_indexer_a_del_cover_art

        self.pg_conn.execute(dedent('''
            DELETE FROM cover_art_archive.cover_art
                WHERE id = 1
        '''))

        self.assertEqual(self.get_event_queue(), [
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

        xml = RELEASE_XML_TEMPLATE.format(
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

        self.session.last_requests = []
        self.session.next_responses = [
            MockResponse(status=204),
            MockResponse(),
            MockResponse(status=200, content=xml),
            MockResponse(status=200, content=xml),
        ]

        indexer.indexer(tests_config, 1,
                        max_idle_loops=1,
                        http_client_cls=self.http_client_cls)

        self.assertEqual(self.session.last_requests, [
            {
                'method': 'DELETE',
                'url': f'http://mbid-{RELEASE1_MBID}.s3.example.com/' +
                       f'mbid-{RELEASE1_MBID}-1.jpg',
                'data': None,
                'headers': {
                    'authorization': 'LOW user:pass',
                    'x-archive-cascade-delete': '1',
                    'x-archive-keep-old-version': '1',
                },
            },
            release_index_json_put(RELEASE1_MBID, []),
            release_mb_metadata_xml_get(RELEASE1_MBID),
            release_mb_metadata_xml_put(RELEASE1_MBID, xml),
        ])

    def test_deleting_release(self):
        # artwork_indexer_a_del_release

        # Queue an index event (via artwork_indexer_a_upd_cover_art).
        # We're checking that it's replaced by the following release
        # deletion event.
        self.pg_conn.execute(dedent('''
            UPDATE cover_art_archive.cover_art
                SET ordering = 3, comment = ''
                WHERE id = 1
        '''))

        new_image1_json = self._orig_image1_json | {'comment': ''}

        self.assertEqual(self.get_event_queue(), [
            release_index_event(RELEASE1_MBID, id=1),
        ])

        # This simulates a merge, where the cover art is first copied to
        # another release, and the original release is deleted.
        self.pg_conn.execute(dedent('''
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

        self.assertEqual(self.get_event_queue(), [
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

        # Make the copy fail. This should halt processing of all dependant
        # events (delete_image, index).
        print('note, the following test is expected to log an HTTP 400 error')
        self.session.last_requests = []
        self.session.next_responses = [
            MockResponse(status=400),
            MockResponse(status=204),
        ]

        self.pg_conn.execute(dedent('''
            UPDATE artwork_indexer.event_queue
            SET attempts = 4, last_updated = (now() - interval '1 day')
            WHERE action = 'copy_image'
        '''))

        indexer.indexer(tests_config, 1,
                        max_idle_loops=1,
                        http_client_cls=self.http_client_cls)

        self.assertEqual(self.get_event_queue(), [
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
            self.pg_conn.execute(dedent('''
                SELECT failure_reason
                FROM artwork_indexer.event_failure_reason
                WHERE event = 5
            ''')).fetchone()['failure_reason'],
            'HTTP 400',
        )

        self.assertEqual(self.session.last_requests, [
            release_image_copy_put(RELEASE1_MBID, RELEASE2_MBID, 1),
            {
                'method': 'DELETE',
                'url': f'http://mbid-{RELEASE1_MBID}.s3.example.com/' +
                       'index.json',
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
        self.pg_conn.execute(dedent('''
            UPDATE artwork_indexer.event_queue
            SET attempts = 0, state = 'queued'
            WHERE action = 'copy_image'
        '''))

        xml = RELEASE_XML_TEMPLATE.format(
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

        self.session.last_requests = []
        self.session.next_responses = [
            MockResponse(),
            MockResponse(status=204),
            MockResponse(),
            MockResponse(status=200, content=xml),
            MockResponse(status=200, content=xml),
        ]

        indexer.indexer(tests_config, 1,
                        max_idle_loops=1,
                        http_client_cls=self.http_client_cls)

        self.assertEqual(self.session.last_requests, [
            release_image_copy_put(RELEASE1_MBID, RELEASE2_MBID, 1),
            {
                'method': 'DELETE',
                'url': f'http://mbid-{RELEASE1_MBID}.s3.example.com/' +
                       f'mbid-{RELEASE1_MBID}-1.jpg',
                'data': None,
                'headers': {
                    'authorization': 'LOW user:pass',
                    'x-archive-cascade-delete': '1',
                    'x-archive-keep-old-version': '1',
                },
            },
            release_index_json_put(RELEASE2_MBID, [new_image1_json]),
            release_mb_metadata_xml_get(RELEASE2_MBID),
            release_mb_metadata_xml_put(RELEASE2_MBID, xml),
        ])

    def test_inserting_cover_art_type(self):
        # artwork_indexer_a_ins_cover_art_type

        self.pg_conn.execute(dedent('''
            INSERT INTO cover_art_archive.cover_art_type (id, type_id)
                VALUES (1, 2);
        '''))

        new_image1_json = (
            self._orig_image1_json |
            {'back': True, 'types': ['Front', 'Back']}
        )

        self._release1_reindex_test(
            event_id=1,
            images_json=[new_image1_json],
            xml_fmt_args={'is_back': 'true'},
        )

    def test_deleting_cover_art_type(self):
        # artwork_indexer_a_del_cover_art_type

        self.pg_conn.execute(dedent('''
            DELETE FROM cover_art_archive.cover_art_type
                WHERE id = 1 AND type_id = 1
        '''))

        new_image1_json = self._orig_image1_json | \
            {'front': False, 'types': []}

        self._release1_reindex_test(
            event_id=1,
            images_json=[new_image1_json],
            xml_fmt_args={'is_front': 'false'},
        )

    def test_updating_artist(self):
        # artwork_indexer_a_upd_artist

        self.pg_conn.execute(dedent('''
            UPDATE artist SET name = 'foo', sort_name = 'bar' WHERE id = 1;
        '''))

        self._release1_reindex_test(
            event_id=1,
            images_json=[self._orig_image1_json],
            xml_fmt_args={
                'artist_name': 'foo',
                'artist_sort_name': 'bar',
            },
        )

    def test_updating_release(self):
        # artwork_indexer_a_upd_release

        self.pg_conn.execute(dedent('''
            UPDATE release SET name = 'updated name1' WHERE id = 1;

            -- Should not produce any update, as there is no cover art
            -- associated with this release.
            UPDATE release SET name = 'updated name2' WHERE id = 2;
        '''))

        self._release1_reindex_test(
            event_id=1,
            images_json=[self._orig_image1_json],
            xml_fmt_args={
                'title': 'updated name1',
            },
        )

    def test_updating_release_meta(self):
        # artwork_indexer_a_upd_release_meta

        self.pg_conn.execute(dedent('''
            UPDATE release_meta SET amazon_asin = 'FOOBAR123' WHERE id = 1;

            -- Should not produce any update, as there is no cover art
            -- associated with this release.
            UPDATE release_meta SET amazon_asin = 'FOOBAR456' WHERE id = 2;
        '''))

        self._release1_reindex_test(
            event_id=1,
            images_json=[self._orig_image1_json],
            xml_fmt_args={
                'asin_xml': '<asin>FOOBAR123</asin>',
            },
        )

    def test_inserting_first_release_date(self):
        # artwork_indexer_a_ins_release_first_release_date

        self.pg_conn.execute(dedent('''
            INSERT INTO release_unknown_country VALUES (1, 1980, 1, 1);

            -- Should not produce any update, as there is no cover art
            -- associated with this release.
            INSERT INTO release_unknown_country VALUES (2, 1970, 1, 1);
        '''))

        self._release1_reindex_test(
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

    def test_deleting_first_release_date(self):
        # artwork_indexer_a_del_release_first_release_date

        self.pg_conn.execute(dedent('''
            DELETE FROM release_country WHERE release = 1;

            -- Should not produce any update, as there is no cover art
            -- associated with this release.
            DELETE FROM release_country WHERE release = 2;
        '''))

        self._release1_reindex_test(
            event_id=1,
            images_json=[self._orig_image1_json],
            xml_fmt_args={
                'release_event_xml': ''
            },
        )


if __name__ == '__main__':
    unittest.main(verbosity=2)
