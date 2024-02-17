INSERT INTO area
        (id, gid, name, type, edits_pending, last_updated,
         begin_date_year, begin_date_month, begin_date_day,
         end_date_year, end_date_month, end_date_day,
         ended, comment)
     VALUES (222, '489ce91b-6658-3307-9877-795b68554c98',
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
     VALUES (1, '16ebbc86-670c-4ad3-980b-bfbd1eee4ff4', 'ⶵ⮮', 1, 1),
            (2, '2198f7b1-658c-4217-8cae-f63abe0b2391', 'artless', 1, 1),
            (3, '41f27dcf-f012-4c91-afc0-0531e196bbda', 'artful', 1, 2);

INSERT INTO release_country
        (release, country, date_year, date_month, date_day)
     VALUES (1, 222, 1989, 10, NULL),
            (2, 222, 1991, NULL, NULL);

INSERT INTO musicbrainz.editor
        (id, name, password, ha1, email, email_confirm_date)
     VALUES (10, 'Editor', '{CLEARTEXT}pass',
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
