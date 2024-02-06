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

    async def asyncSetUp(self):
        await super().asyncSetUp()

        await self.pg_conn.execute(dedent('''
            INSERT INTO area
                    (id, gid, name, type, edits_pending, last_updated,
                     begin_date_year, begin_date_month, begin_date_day,
                     end_date_year, end_date_month, end_date_day,
                     ended, comment)
                VALUES
                    (222, '489ce91b-6658-3307-9877-795b68554c98',
                     'United States', 1, 0, '2013-06-15 18:06:39.59323+00',
                      NULL, NULL, NULL, NULL, NULL, NULL, '0', '');

            INSERT INTO country_area (area)
                VALUES (222);

            INSERT INTO iso_3166_1 (area, code)
                VALUES (222, 'US');

            INSERT INTO musicbrainz.artist (id, gid, name, sort_name)
                VALUES (1, 'ae859a2d-5754-4e88-9af0-6df263345535', '🀽', '🀽'),
                       (2, '4698a32d-b014-4da6-bdb7-de59fa5179bc', 'O', 'O');

            INSERT INTO musicbrainz.artist_credit
                    (id, gid, name, artist_count)
                VALUES (1, '87d69648-5604-4237-929d-6d2774867811', '✺⧳', 1),
                       (2, '10823f6d-546f-49cc-bc74-0d1095666186', 'O', 1);

            INSERT INTO musicbrainz.artist_credit_name
                    (artist_credit, name, artist, position)
                VALUES (1, '✺⧳', 1, 1),
                       (2, 'O', 2, 1);

            INSERT INTO musicbrainz.release_group
                    (id, gid, name, artist_credit)
                VALUES (1, '9fc47cc7-7a57-4248-b194-75cacadd3646', '⟦⯛', 1);

            INSERT INTO musicbrainz.release
                    (id, gid, name, release_group, artist_credit)
                VALUES
                    (1, '16ebbc86-670c-4ad3-980b-bfbd1eee4ff4', 'ⶵ⮮',
                     1, 1),
                    (2, '2198f7b1-658c-4217-8cae-f63abe0b2391', 'artless',
                     1, 1),
                    (3, '41f27dcf-f012-4c91-afc0-0531e196bbda', 'artful',
                     1, 2);

            INSERT INTO release_country
                    (release, country, date_year, date_month, date_day)
                VALUES (1, 222, 1989, 10, NULL),
                       (2, 222, 1991, NULL, NULL);

            INSERT INTO musicbrainz.editor
                    (id, name, password, ha1, email, email_confirm_date)
                VALUES
                    (10, 'Editor', '{CLEARTEXT}pass',
                     'b5ba49bbd92eb35ddb35b5acd039440d',
                     'Editor@example.com', now());

            INSERT INTO musicbrainz.edit
                    (id, editor, type, status, expire_time)
                VALUES (1, 10, 314, 2, now()),
                       (2, 10, 314, 2, now());

            INSERT INTO musicbrainz.edit_data (edit, data)
                VALUES (1, '{}'),
                       (2, '{}');

            INSERT INTO cover_art_archive.cover_art
                    (id, release, mime_type, edit, ordering, comment)
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

        await indexer.indexer(tests_config, 1,
                              max_idle_loops=1,
                              http_client_cls=self.http_client_cls)

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
            INSERT INTO musicbrainz.edit
                    (id, editor, type, status, expire_time)
                VALUES (3, 10, 314, 2, now());

            INSERT INTO musicbrainz.edit_data (edit, data)
                VALUES (3, '{}');

            INSERT INTO cover_art_archive.cover_art
                    (id, release, mime_type, edit, ordering, comment)
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
        # artwork_indexer_a_upd_cover_art

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
        # artwork_indexer_a_del_cover_art

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

        await indexer.indexer(tests_config, 1,
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
        # artwork_indexer_a_del_release

        # Queue an index event (via artwork_indexer_a_upd_cover_art).
        # We're checking that it's replaced by the following release
        # deletion event.
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
        # artwork_indexer_a_ins_cover_art_type

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
        # artwork_indexer_a_del_cover_art_type

        await self.pg_conn.execute(dedent('''
            DELETE FROM cover_art_archive.cover_art_type
                WHERE id = 1 AND type_id = 1
        '''))

        new_image1_json = self._orig_image1_json | \
            {'front': False, 'types': []}

        await self._release1_reindex_test(
            event_id=1,
            images_json=[new_image1_json],
            xml_fmt_args={'is_front': 'false'},
        )

    async def test_updating_artist(self):
        # artwork_indexer_a_upd_artist

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
        # artwork_indexer_a_upd_release

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
        # artwork_indexer_a_upd_release_meta

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
        # artwork_indexer_a_ins_release_first_release_date

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
        # artwork_indexer_a_del_release_first_release_date

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

        await indexer.indexer(tests_config, 1,
                              max_idle_loops=1,
                              http_client_cls=self.http_client_cls)

        event_count = await self.pg_conn.fetchval(dedent('''
            SELECT count(*) FROM artwork_indexer.event_queue
        '''))
        self.assertEqual(event_count, 1)

        await self.pg_conn.execute(dedent('''
            UPDATE artwork_indexer.event_queue
            SET created = (created - interval '90 days');
        '''))

        await indexer.indexer(tests_config, 2,
                              max_idle_loops=2,
                              http_client_cls=self.http_client_cls)

        event_count = await self.pg_conn.fetchval(dedent('''
            SELECT count(*) FROM artwork_indexer.event_queue
        '''))
        self.assertEqual(event_count, 0)


if __name__ == '__main__':
    unittest.main(verbosity=2)
