\set ON_ERROR_STOP 1

BEGIN;

SET search_path = 'cover_art_archive';

CREATE OR REPLACE FUNCTION reindex_release() RETURNS trigger AS $$
DECLARE
    release_mbid UUID;
BEGIN
    SELECT gid INTO release_mbid
    FROM musicbrainz.release r
    JOIN cover_art_archive.cover_art caa_r ON r.id = caa_r.release
    WHERE r.id = NEW.id;

    IF FOUND THEN
        INSERT INTO artwork_indexer.event_queue (entity_type, action, message)
        VALUES ('release', 'index', jsonb_build_object('gid', release_mbid))
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
        SELECT 'release', 'index', jsonb_build_object('gid', r.gid)
        FROM musicbrainz.release r
        JOIN cover_art_archive.cover_art caa_r ON r.id = caa_r.release
        JOIN musicbrainz.artist_credit_name acn ON r.artist_credit = acn.artist_credit
        WHERE acn.artist = NEW.id
    )
    ON CONFLICT DO NOTHING;

    RETURN NULL;
END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION reindex_caa() RETURNS trigger AS $$
BEGIN
    INSERT INTO artwork_indexer.event_queue (entity_type, action, message) (
        SELECT 'release', 'index', jsonb_build_object('gid', gid::text)
        FROM musicbrainz.release
        WHERE id = coalesce((
            CASE TG_OP
                WHEN 'DELETE' THEN OLD.release
                ELSE NEW.release
            END
        ))
    )
    ON CONFLICT DO NOTHING;
    RETURN NULL;
END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION reindex_caa_type() RETURNS trigger AS $$
BEGIN
    INSERT INTO artwork_indexer.event_queue (entity_type, action, message) (
        SELECT 'release', 'index', jsonb_build_object('gid', r.gid::text)
        FROM musicbrainz.release r
        JOIN cover_art_archive.cover_art ca ON r.id = ca.release
        WHERE ca.id = coalesce((
            CASE TG_OP
                WHEN 'DELETE' THEN OLD.id
                ELSE NEW.id
            END
        ))
    )
    ON CONFLICT DO NOTHING;
    RETURN NULL;
END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION move_release() RETURNS trigger AS $$
BEGIN
    IF OLD.release != NEW.release THEN
        INSERT INTO artwork_indexer.event_queue (entity_type, action, message) (
            SELECT 'release', 'move_image', jsonb_build_object(
                'artwork_id', ca.id,
                'old_gid', old_release.gid,
                'new_gid', new_release.gid,
                'suffix', it.suffix
            )
            FROM cover_art_archive.cover_art ca
            JOIN cover_art_archive.image_type it ON it.mime_type = ca.mime_type
            JOIN musicbrainz.release old_release ON old_release.id = OLD.release
            JOIN musicbrainz.release new_release ON new_release.id = NEW.release
            WHERE ca.id = OLD.id
        )
        ON CONFLICT DO NOTHING;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION delete_release() RETURNS trigger AS $$
BEGIN
    INSERT INTO artwork_indexer.event_queue (entity_type, action, message) (
        SELECT 'release', 'delete_image', jsonb_build_object(
            'artwork_id', ca.id,
            'gid', OLD.gid,
            'suffix', it.suffix
        )
        FROM cover_art_archive.cover_art ca
        JOIN cover_art_archive.image_type it ON it.mime_type = ca.mime_type
        WHERE ca.release = OLD.id
    )
    ON CONFLICT DO NOTHING;

    INSERT INTO artwork_indexer.event_queue (entity_type, action, message)
    VALUES ('release', 'deindex', jsonb_build_object('gid', OLD.gid))
    ON CONFLICT DO NOTHING;

    RETURN OLD;
END;
$$ LANGUAGE 'plpgsql';

COMMIT;
