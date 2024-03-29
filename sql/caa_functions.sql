-- Automatically generated, do not edit.

CREATE OR REPLACE FUNCTION artwork_indexer.a_ins_cover_art() RETURNS trigger AS $$
DECLARE
    release_gid UUID;
BEGIN
    SELECT musicbrainz.release.gid
    INTO STRICT release_gid
    FROM musicbrainz.release
    WHERE musicbrainz.release.id = NEW.release;

    INSERT INTO artwork_indexer.event_queue (entity_type, action, message) VALUES ('release', 'index', jsonb_build_object('gid', release_gid)) ON CONFLICT DO NOTHING;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION artwork_indexer.b_upd_cover_art() RETURNS trigger AS $$
DECLARE
    suffix TEXT;
    old_release_gid UUID;
    new_release_gid UUID;
    copy_event_id BIGINT;
    delete_event_id BIGINT;
BEGIN
    SELECT cover_art_archive.image_type.suffix, old_release.gid, new_release.gid
    INTO STRICT suffix, old_release_gid, new_release_gid
    FROM cover_art_archive.cover_art
    JOIN cover_art_archive.image_type USING (mime_type)
    JOIN musicbrainz.release old_release ON old_release.id = OLD.release
    JOIN musicbrainz.release new_release ON new_release.id = NEW.release
    WHERE cover_art_archive.cover_art.id = OLD.id;

    IF OLD.release != NEW.release THEN
        -- The release column changed, meaning two entities were merged.
        -- We'll copy the image to the new release and delete it from
        -- the old one. The deletion event should have the copy event as its
        -- parent, so that it doesn't run until that completes.
        --
        -- We have no ON CONFLICT specifiers on the copy_image or delete_image,
        -- events, because they should *not* conflict with any existing event.

        INSERT INTO artwork_indexer.event_queue (entity_type, action, message)
        VALUES ('release', 'copy_image', jsonb_build_object(
            'artwork_id', OLD.id,
            'old_gid', old_release_gid,
            'new_gid', new_release_gid,
            'suffix', suffix
        ))
        RETURNING id INTO STRICT copy_event_id;

        INSERT INTO artwork_indexer.event_queue (entity_type, action, message, depends_on) VALUES ('release', 'delete_image', jsonb_build_object('artwork_id', OLD.id, 'gid', old_release_gid, 'suffix', suffix), array[copy_event_id]) RETURNING id INTO STRICT delete_event_id;

        -- Check if any images remain for the old release. If not, deindex it.
        PERFORM 1 FROM cover_art_archive.cover_art
        WHERE cover_art_archive.cover_art.release = OLD.release
        AND cover_art_archive.cover_art.id != OLD.id
        LIMIT 1;

        IF FOUND THEN
            -- If there's an existing, queued index event, reset its parent to our
            -- deletion event (i.e. delay it until after the deletion executes).
            INSERT INTO artwork_indexer.event_queue (entity_type, action, message, depends_on) VALUES ('release', 'index', jsonb_build_object('gid', old_release_gid), array[delete_event_id]), ('release', 'index', jsonb_build_object('gid', new_release_gid), array[delete_event_id]) ON CONFLICT (entity_type, action, message) WHERE state = 'queued' DO UPDATE SET depends_on = (coalesce(artwork_indexer.event_queue.depends_on, '{}') || delete_event_id);
        ELSE
            INSERT INTO artwork_indexer.event_queue (entity_type, action, message, depends_on) VALUES ('release', 'index', jsonb_build_object('gid', new_release_gid), array[delete_event_id]) ON CONFLICT (entity_type, action, message) WHERE state = 'queued' DO UPDATE SET depends_on = (coalesce(artwork_indexer.event_queue.depends_on, '{}') || delete_event_id);
            INSERT INTO artwork_indexer.event_queue (entity_type, action, message, depends_on) VALUES ('release', 'deindex', jsonb_build_object('gid', old_release_gid), array[delete_event_id]) ON CONFLICT DO NOTHING;
            DELETE FROM artwork_indexer.event_queue WHERE state = 'queued' AND entity_type = 'release' AND action = 'index' AND message = jsonb_build_object('gid', old_release_gid);
        END IF;
    ELSE
        INSERT INTO artwork_indexer.event_queue (entity_type, action, message) VALUES ('release', 'index', jsonb_build_object('gid', old_release_gid)), ('release', 'index', jsonb_build_object('gid', new_release_gid)) ON CONFLICT DO NOTHING;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION artwork_indexer.b_del_cover_art()
RETURNS trigger AS $$
DECLARE
    suffix TEXT;
    release_gid UUID;
    delete_event_id BIGINT;
BEGIN
    SELECT cover_art_archive.image_type.suffix, musicbrainz.release.gid
    INTO suffix, release_gid
    FROM musicbrainz.release
    JOIN cover_art_archive.image_type ON cover_art_archive.image_type.mime_type = OLD.mime_type
    WHERE musicbrainz.release.id = OLD.release;

    -- If no row is found, it's likely because the entity itself has been
    -- deleted, which cascades to this table.
    IF FOUND THEN
        INSERT INTO artwork_indexer.event_queue (entity_type, action, message) VALUES ('release', 'delete_image', jsonb_build_object('artwork_id', OLD.id, 'gid', release_gid, 'suffix', suffix)) RETURNING id INTO STRICT delete_event_id;
        INSERT INTO artwork_indexer.event_queue (entity_type, action, message, depends_on) VALUES ('release', 'index', jsonb_build_object('gid', release_gid), array[delete_event_id]) ON CONFLICT (entity_type, action, message) WHERE state = 'queued' DO UPDATE SET depends_on = (coalesce(artwork_indexer.event_queue.depends_on, '{}') || delete_event_id);
    END IF;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION artwork_indexer.a_ins_cover_art_type() RETURNS trigger AS $$
DECLARE
    release_gid UUID;
BEGIN
    SELECT musicbrainz.release.gid
    INTO STRICT release_gid
    FROM musicbrainz.release
    JOIN cover_art_archive.cover_art ON musicbrainz.release.id = cover_art_archive.cover_art.release
    WHERE cover_art_archive.cover_art.id = NEW.id;

    INSERT INTO artwork_indexer.event_queue (entity_type, action, message) VALUES ('release', 'index', jsonb_build_object('gid', release_gid)) ON CONFLICT DO NOTHING;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION artwork_indexer.b_del_cover_art_type() RETURNS trigger AS $$
DECLARE
    release_gid UUID;
BEGIN
    SELECT musicbrainz.release.gid
    INTO release_gid
    FROM musicbrainz.release
    JOIN cover_art_archive.cover_art ON musicbrainz.release.id = cover_art_archive.cover_art.release
    WHERE cover_art_archive.cover_art.id = OLD.id;

    -- If no row is found, it's likely because the artwork itself has been
    -- deleted, which cascades to this table.
    IF FOUND THEN
        INSERT INTO artwork_indexer.event_queue (entity_type, action, message) VALUES ('release', 'index', jsonb_build_object('gid', release_gid)) ON CONFLICT DO NOTHING;
    END IF;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION artwork_indexer.b_del_release() RETURNS trigger AS $$
BEGIN
    PERFORM 1 FROM cover_art_archive.cover_art
    WHERE cover_art_archive.cover_art.release = OLD.id
    LIMIT 1;

    IF FOUND THEN
        INSERT INTO artwork_indexer.event_queue (entity_type, action, message) (
            SELECT 'release', 'delete_image',
                jsonb_build_object(
                    'artwork_id', cover_art_archive.cover_art.id,
                    'gid', OLD.gid,
                    'suffix', cover_art_archive.image_type.suffix
                )
            FROM cover_art_archive.cover_art
            JOIN cover_art_archive.image_type USING (mime_type)
            WHERE cover_art_archive.cover_art.release = OLD.id
        )
        ON CONFLICT DO NOTHING;
        INSERT INTO artwork_indexer.event_queue (entity_type, action, message, depends_on) VALUES ('release', 'deindex', jsonb_build_object('gid', OLD.gid), NULL) ON CONFLICT DO NOTHING;
        DELETE FROM artwork_indexer.event_queue WHERE state = 'queued' AND entity_type = 'release' AND action = 'index' AND message = jsonb_build_object('gid', OLD.gid);
    END IF;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION artwork_indexer.a_upd_artist() RETURNS trigger AS $$
BEGIN
    IF (OLD.name != NEW.name OR OLD.sort_name != NEW.sort_name) THEN
        INSERT INTO artwork_indexer.event_queue (entity_type, action, message) (
            SELECT 'release', 'index', jsonb_build_object('gid', musicbrainz.release.gid)
            FROM musicbrainz.release
            JOIN musicbrainz.artist_credit_name ON musicbrainz.artist_credit_name.artist_credit = musicbrainz.release.artist_credit
            WHERE EXISTS (
                SELECT 1 FROM cover_art_archive.cover_art
                WHERE cover_art_archive.cover_art.release = musicbrainz.release.id
            )
            AND musicbrainz.artist_credit_name.artist = NEW.id
        )
        ON CONFLICT DO NOTHING;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION artwork_indexer.a_upd_release() RETURNS trigger AS $$
BEGIN
    IF (OLD.name != NEW.name OR OLD.artist_credit != NEW.artist_credit OR OLD.language IS DISTINCT FROM NEW.language OR OLD.barcode IS DISTINCT FROM NEW.barcode) THEN
        INSERT INTO artwork_indexer.event_queue (entity_type, action, message) (
            SELECT 'release', 'index', jsonb_build_object('gid', musicbrainz.release.gid)
            FROM musicbrainz.release
            WHERE EXISTS (
                SELECT 1 FROM cover_art_archive.cover_art
                WHERE cover_art_archive.cover_art.release = musicbrainz.release.id
            )
            AND musicbrainz.release.gid = NEW.gid
        )
        ON CONFLICT DO NOTHING;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION artwork_indexer.a_upd_release_meta() RETURNS trigger AS $$
BEGIN
    IF (OLD.amazon_asin IS DISTINCT FROM NEW.amazon_asin) THEN
        INSERT INTO artwork_indexer.event_queue (entity_type, action, message) (
            SELECT 'release', 'index', jsonb_build_object('gid', musicbrainz.release.gid)
            FROM musicbrainz.release
            WHERE EXISTS (
                SELECT 1 FROM cover_art_archive.cover_art
                WHERE cover_art_archive.cover_art.release = musicbrainz.release.id
            )
            AND musicbrainz.release.id = NEW.id
        )
        ON CONFLICT DO NOTHING;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION artwork_indexer.a_ins_release_first_release_date() RETURNS trigger AS $$
BEGIN
    INSERT INTO artwork_indexer.event_queue (entity_type, action, message) (
        SELECT 'release', 'index', jsonb_build_object('gid', musicbrainz.release.gid)
        FROM musicbrainz.release
        WHERE EXISTS (
            SELECT 1 FROM cover_art_archive.cover_art
            WHERE cover_art_archive.cover_art.release = musicbrainz.release.id
        )
        AND musicbrainz.release.id = NEW.release
    )
    ON CONFLICT DO NOTHING;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION artwork_indexer.a_del_release_first_release_date() RETURNS trigger AS $$
BEGIN
    INSERT INTO artwork_indexer.event_queue (entity_type, action, message) (
        SELECT 'release', 'index', jsonb_build_object('gid', musicbrainz.release.gid)
        FROM musicbrainz.release
        WHERE EXISTS (
            SELECT 1 FROM cover_art_archive.cover_art
            WHERE cover_art_archive.cover_art.release = musicbrainz.release.id
        )
        AND musicbrainz.release.id = OLD.release
    )
    ON CONFLICT DO NOTHING;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

