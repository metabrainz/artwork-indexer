-- Automatically generated, do not edit.

CREATE OR REPLACE FUNCTION artwork_indexer.a_ins_event_art() RETURNS trigger AS $$
DECLARE
    event_gid UUID;
BEGIN
    SELECT musicbrainz.event.gid
    INTO STRICT event_gid
    FROM musicbrainz.event
    WHERE musicbrainz.event.id = NEW.event;

    INSERT INTO artwork_indexer.event_queue (entity_type, action, message) VALUES ('event', 'index', jsonb_build_object('gid', event_gid)) ON CONFLICT DO NOTHING;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION artwork_indexer.b_upd_event_art() RETURNS trigger AS $$
DECLARE
    suffix TEXT;
    old_event_gid UUID;
    new_event_gid UUID;
    copy_event_id BIGINT;
    delete_event_id BIGINT;
BEGIN
    SELECT cover_art_archive.image_type.suffix, old_event.gid, new_event.gid
    INTO STRICT suffix, old_event_gid, new_event_gid
    FROM event_art_archive.event_art
    JOIN cover_art_archive.image_type USING (mime_type)
    JOIN musicbrainz.event old_event ON old_event.id = OLD.event
    JOIN musicbrainz.event new_event ON new_event.id = NEW.event
    WHERE event_art_archive.event_art.id = OLD.id;

    IF OLD.event != NEW.event THEN
        -- The event column changed, meaning two entities were merged.
        -- We'll copy the image to the new event and delete it from
        -- the old one. The deletion event should have the copy event as its
        -- parent, so that it doesn't run until that completes.
        --
        -- We have no ON CONFLICT specifiers on the copy_image or delete_image,
        -- events, because they should *not* conflict with any existing event.

        INSERT INTO artwork_indexer.event_queue (entity_type, action, message)
        VALUES ('event', 'copy_image', jsonb_build_object(
            'artwork_id', OLD.id,
            'old_gid', old_event_gid,
            'new_gid', new_event_gid,
            'suffix', suffix
        ))
        RETURNING id INTO STRICT copy_event_id;

        INSERT INTO artwork_indexer.event_queue (entity_type, action, message, depends_on) VALUES ('event', 'delete_image', jsonb_build_object('artwork_id', OLD.id, 'gid', old_event_gid, 'suffix', suffix), array[copy_event_id]) RETURNING id INTO STRICT delete_event_id;

        -- Check if any images remain for the old event. If not, deindex it.
        PERFORM 1 FROM event_art_archive.event_art
        WHERE event_art_archive.event_art.event = OLD.event
        AND event_art_archive.event_art.id != OLD.id
        LIMIT 1;

        IF FOUND THEN
            -- If there's an existing, queued index event, reset its parent to our
            -- deletion event (i.e. delay it until after the deletion executes).
            INSERT INTO artwork_indexer.event_queue (entity_type, action, message, depends_on) VALUES ('event', 'index', jsonb_build_object('gid', old_event_gid), array[delete_event_id]), ('event', 'index', jsonb_build_object('gid', new_event_gid), array[delete_event_id]) ON CONFLICT (entity_type, action, message) WHERE state = 'queued' DO UPDATE SET depends_on = (coalesce(artwork_indexer.event_queue.depends_on, '{}') || delete_event_id);
        ELSE
            INSERT INTO artwork_indexer.event_queue (entity_type, action, message, depends_on) VALUES ('event', 'index', jsonb_build_object('gid', new_event_gid), array[delete_event_id]) ON CONFLICT (entity_type, action, message) WHERE state = 'queued' DO UPDATE SET depends_on = (coalesce(artwork_indexer.event_queue.depends_on, '{}') || delete_event_id);
            INSERT INTO artwork_indexer.event_queue (entity_type, action, message, depends_on) VALUES ('event', 'deindex', jsonb_build_object('gid', old_event_gid), array[delete_event_id]) ON CONFLICT DO NOTHING;
            DELETE FROM artwork_indexer.event_queue WHERE state = 'queued' AND entity_type = 'event' AND action = 'index' AND message = jsonb_build_object('gid', old_event_gid);
        END IF;
    ELSE
        INSERT INTO artwork_indexer.event_queue (entity_type, action, message) VALUES ('event', 'index', jsonb_build_object('gid', old_event_gid)), ('event', 'index', jsonb_build_object('gid', new_event_gid)) ON CONFLICT DO NOTHING;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION artwork_indexer.b_del_event_art()
RETURNS trigger AS $$
DECLARE
    suffix TEXT;
    event_gid UUID;
    delete_event_id BIGINT;
BEGIN
    SELECT cover_art_archive.image_type.suffix, musicbrainz.event.gid
    INTO suffix, event_gid
    FROM musicbrainz.event
    JOIN cover_art_archive.image_type ON cover_art_archive.image_type.mime_type = OLD.mime_type
    WHERE musicbrainz.event.id = OLD.event;

    -- If no row is found, it's likely because the entity itself has been
    -- deleted, which cascades to this table.
    IF FOUND THEN
        INSERT INTO artwork_indexer.event_queue (entity_type, action, message) VALUES ('event', 'delete_image', jsonb_build_object('artwork_id', OLD.id, 'gid', event_gid, 'suffix', suffix)) RETURNING id INTO STRICT delete_event_id;
        INSERT INTO artwork_indexer.event_queue (entity_type, action, message, depends_on) VALUES ('event', 'index', jsonb_build_object('gid', event_gid), array[delete_event_id]) ON CONFLICT (entity_type, action, message) WHERE state = 'queued' DO UPDATE SET depends_on = (coalesce(artwork_indexer.event_queue.depends_on, '{}') || delete_event_id);
    END IF;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION artwork_indexer.a_ins_event_art_type() RETURNS trigger AS $$
DECLARE
    event_gid UUID;
BEGIN
    SELECT musicbrainz.event.gid
    INTO STRICT event_gid
    FROM musicbrainz.event
    JOIN event_art_archive.event_art ON musicbrainz.event.id = event_art_archive.event_art.event
    WHERE event_art_archive.event_art.id = NEW.id;

    INSERT INTO artwork_indexer.event_queue (entity_type, action, message) VALUES ('event', 'index', jsonb_build_object('gid', event_gid)) ON CONFLICT DO NOTHING;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION artwork_indexer.b_del_event_art_type() RETURNS trigger AS $$
DECLARE
    event_gid UUID;
BEGIN
    SELECT musicbrainz.event.gid
    INTO event_gid
    FROM musicbrainz.event
    JOIN event_art_archive.event_art ON musicbrainz.event.id = event_art_archive.event_art.event
    WHERE event_art_archive.event_art.id = OLD.id;

    -- If no row is found, it's likely because the artwork itself has been
    -- deleted, which cascades to this table.
    IF FOUND THEN
        INSERT INTO artwork_indexer.event_queue (entity_type, action, message) VALUES ('event', 'index', jsonb_build_object('gid', event_gid)) ON CONFLICT DO NOTHING;
    END IF;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION artwork_indexer.b_del_event() RETURNS trigger AS $$
BEGIN
    PERFORM 1 FROM event_art_archive.event_art
    WHERE event_art_archive.event_art.event = OLD.id
    LIMIT 1;

    IF FOUND THEN
        INSERT INTO artwork_indexer.event_queue (entity_type, action, message) (
            SELECT 'event', 'delete_image',
                jsonb_build_object(
                    'artwork_id', event_art_archive.event_art.id,
                    'gid', OLD.gid,
                    'suffix', cover_art_archive.image_type.suffix
                )
            FROM event_art_archive.event_art
            JOIN cover_art_archive.image_type USING (mime_type)
            WHERE event_art_archive.event_art.event = OLD.id
        )
        ON CONFLICT DO NOTHING;
        INSERT INTO artwork_indexer.event_queue (entity_type, action, message, depends_on) VALUES ('event', 'deindex', jsonb_build_object('gid', OLD.gid), NULL) ON CONFLICT DO NOTHING;
        DELETE FROM artwork_indexer.event_queue WHERE state = 'queued' AND entity_type = 'event' AND action = 'index' AND message = jsonb_build_object('gid', OLD.gid);
    END IF;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION artwork_indexer.a_upd_event() RETURNS trigger AS $$
BEGIN
    IF (OLD.name != NEW.name) THEN
        INSERT INTO artwork_indexer.event_queue (entity_type, action, message) (
            SELECT 'event', 'index', jsonb_build_object('gid', musicbrainz.event.gid)
            FROM musicbrainz.event
            WHERE EXISTS (
                SELECT 1 FROM event_art_archive.event_art
                WHERE event_art_archive.event_art.event = musicbrainz.event.id
            )
            AND musicbrainz.event.gid = NEW.gid
        )
        ON CONFLICT DO NOTHING;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

