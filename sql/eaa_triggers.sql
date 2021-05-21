\set ON_ERROR_STOP 1

BEGIN;

SET search_path = 'event_art_archive';

-- Simulate "CREATE OR REPLACE," which isn't implemented
-- for "CREATE TRIGGER."

DROP TRIGGER IF EXISTS eaa_reindex ON musicbrainz.event;
DROP TRIGGER IF EXISTS eaa_reindex ON musicbrainz.artist;
DROP TRIGGER IF EXISTS eaa_reindex ON musicbrainz.l_artist_event;
DROP TRIGGER IF EXISTS eaa_reindex ON musicbrainz.place;
DROP TRIGGER IF EXISTS eaa_reindex ON musicbrainz.l_event_place;
DROP TRIGGER IF EXISTS eaa_reindex ON musicbrainz.event_art;
DROP TRIGGER IF EXISTS eaa_move ON musicbrainz.event_art;
DROP TRIGGER IF EXISTS eaa_delete ON musicbrainz.event;

CREATE TRIGGER eaa_reindex AFTER UPDATE OR INSERT
    ON musicbrainz.event FOR EACH ROW
    EXECUTE PROCEDURE reindex_event();

CREATE TRIGGER eaa_reindex AFTER UPDATE
    ON musicbrainz.artist FOR EACH ROW
    EXECUTE PROCEDURE reindex_artist();

CREATE TRIGGER eaa_reindex AFTER UPDATE OR INSERT OR DELETE
    ON musicbrainz.l_artist_event FOR EACH ROW
    EXECUTE PROCEDURE reindex_l_artist_event();

CREATE TRIGGER eaa_reindex AFTER UPDATE
    ON musicbrainz.place FOR EACH ROW
    EXECUTE PROCEDURE reindex_place();

CREATE TRIGGER eaa_reindex AFTER UPDATE OR INSERT OR DELETE
    ON musicbrainz.l_event_place FOR EACH ROW
    EXECUTE PROCEDURE reindex_l_event_place();

CREATE TRIGGER eaa_reindex AFTER UPDATE OR INSERT OR DELETE
    ON event_art_archive.event_art FOR EACH ROW
    EXECUTE PROCEDURE reindex_eaa();

CREATE TRIGGER eaa_move BEFORE UPDATE
    ON event_art_archive.event_art FOR EACH ROW
    EXECUTE PROCEDURE move_event();

CREATE TRIGGER eaa_delete BEFORE DELETE
    ON musicbrainz.event FOR EACH ROW
    EXECUTE PROCEDURE delete_event();

COMMIT;
