\set ON_ERROR_STOP 1

BEGIN;

SET search_path = 'event_art_archive';

CREATE OR REPLACE FUNCTION reindex_event() RETURNS trigger AS $$
DECLARE
  event_mbid UUID;
BEGIN
    SELECT gid INTO event_mbid
    FROM musicbrainz.event e
    JOIN event_art_archive.event_art ea ON e.id = ea.event
    WHERE e.id = NEW.id;

    IF FOUND THEN
        INSERT INTO artwork_indexer.event_queue (entity_type, action, message)
        VALUES ('event', 'index', jsonb_build_object('gid', event_mbid))
        ON CONFLICT DO NOTHING;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION reindex_artist() RETURNS trigger AS $$
BEGIN
    -- Short circuit if the name hasn't changed
    IF NEW.name = OLD.name AND NEW.sort_name = OLD.sort_name THEN
        RETURN NULL;
    END IF;

    INSERT INTO artwork_indexer.event_queue (entity_type, action, message) (
        SELECT 'event', 'index', jsonb_build_object('gid', e.gid)
        FROM musicbrainz.event e
        JOIN event_art_archive.event_art ea ON e.id = ea.event
        JOIN musicbrainz.l_artist_event lae ON e.id = lae.entity1
        WHERE lae.entity0 = NEW.id
    )
    ON CONFLICT DO NOTHING;

    RETURN NULL;
END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION reindex_l_artist_event() RETURNS trigger AS $$
BEGIN
    INSERT INTO artwork_indexer.event_queue (entity_type, action, message) (
        SELECT 'event', 'index', jsonb_build_object('gid', e.gid)
        FROM musicbrainz.event e
        JOIN event_art_archive.event_art ea ON e.id = ea.event
        JOIN musicbrainz.l_artist_event lae ON e.id = lae.entity1
        WHERE lae.id = (
            CASE TG_OP
                WHEN 'DELETE' THEN OLD.id
                ELSE NEW.id
            END
        )
    )
    ON CONFLICT DO NOTHING;
    RETURN NULL;
END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION reindex_place() RETURNS trigger AS $$
BEGIN
    INSERT INTO artwork_indexer.event_queue (entity_type, action, message) (
        SELECT 'event', 'index', jsonb_build_object('gid', e.gid)
        FROM musicbrainz.event e
        JOIN event_art_archive.event_art ea ON e.id = ea.event
        JOIN musicbrainz.l_event_place lep ON e.id = lep.entity0
        WHERE lep.entity1 = NEW.id
    )
    ON CONFLICT DO NOTHING;
    RETURN NULL;
END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION reindex_l_event_place() RETURNS trigger AS $$
BEGIN
    INSERT INTO artwork_indexer.event_queue (entity_type, action, message) (
        SELECT 'event', 'index', jsonb_build_object('gid', e.gid)
        FROM musicbrainz.event e
        JOIN event_art_archive.event_art ea ON e.id = ea.event
        JOIN musicbrainz.l_event_place lep ON e.id = lep.entity0
        WHERE lep.id = (
            CASE TG_OP
                WHEN 'DELETE' THEN OLD.id
                ELSE NEW.id
            END
        )
    )
    ON CONFLICT DO NOTHING;
    RETURN NULL;
END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION reindex_eaa() RETURNS trigger AS $$
BEGIN
    INSERT INTO artwork_indexer.event_queue (entity_type, action, message) (
        SELECT 'event', 'index', jsonb_build_object('gid', e.gid)
        FROM musicbrainz.event
        WHERE id = coalesce((
            CASE TG_OP
                WHEN 'DELETE' THEN OLD.event
                ELSE NEW.event
            END
        ))
    )
    ON CONFLICT DO NOTHING;
    RETURN NULL;
END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION move_event() RETURNS trigger AS $$
BEGIN
    IF OLD.event != NEW.event THEN
        INSERT INTO artwork_indexer.event_queue (entity_type, action, message) (
            SELECT 'event', 'move_image', jsonb_build_object(
                'artwork_id', ea.id,
                'old_gid', old_event.gid,
                'new_gid', new_event.gid,
                'suffix', it.suffix
            )
            FROM event_art_archive.event_art ea
            JOIN event_art_archive.image_type it ON it.mime_type = ea.mime_type
            JOIN musicbrainz.event old_event ON old_event.id = OLD.event
            JOIN musicbrainz.event new_event ON new_event.id = NEW.event
            WHERE ea.id = OLD.id
        )
        ON CONFLICT DO NOTHING;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION delete_event() RETURNS trigger AS $$
BEGIN
    INSERT INTO artwork_indexer.event_queue (entity_type, action, message) (
        SELECT 'event', 'delete_image', jsonb_build_object(
            'artwork_id', ea.id,
            'gid', OLD.gid,
            'suffix', it.suffix
        )
        FROM event_art_archive.event_art ea
        JOIN event_art_archive.image_type it ON it.mime_type = ea.mime_type
        WHERE ea.event = OLD.id
      )
    ON CONFLICT DO NOTHING;

    INSERT INTO artwork_indexer.event_queue (entity_type, action, message)
    VALUES ('event', 'deindex', jsonb_build_object('gid', OLD.gid))
    ON CONFLICT DO NOTHING;

    RETURN OLD;
END;
$$ LANGUAGE 'plpgsql';

COMMIT;
