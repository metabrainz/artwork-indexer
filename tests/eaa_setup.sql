INSERT INTO musicbrainz.event
        (id, gid, name, begin_date_year, end_date_year,
         time, type)
     VALUES (1, 'e2aad65a-12e0-44ec-b693-94d225154e90',
             'live at the place 1', 1990, 1990, '20:00', 1),
            (2, 'a0f19ff3-e140-417f-81c6-2a7466eeea0a',
             'live at the place 2', 1991, 1991, '21:00', 1);

INSERT INTO musicbrainz.editor
        (id, name, password, ha1, email, email_confirm_date)
     VALUES (10, 'Editor', '{CLEARTEXT}pass',
             'b5ba49bbd92eb35ddb35b5acd039440d',
             'Editor@example.com', now());

INSERT INTO musicbrainz.edit (id, editor, type, status, expire_time)
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
