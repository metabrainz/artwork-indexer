\set ON_ERROR_STOP 1

BEGIN;

SET search_path = 'cover_art_archive';

-- Simulate "CREATE OR REPLACE," which isn't implemented
-- for "CREATE TRIGGER."

DROP TRIGGER IF EXISTS caa_reindex ON musicbrainz.release;
DROP TRIGGER IF EXISTS caa_reindex ON musicbrainz.artist;
DROP TRIGGER IF EXISTS caa_reindex ON cover_art_archive.cover_art;
DROP TRIGGER IF EXISTS caa_reindex ON cover_art_archive.cover_art_type;
DROP TRIGGER IF EXISTS caa_move ON cover_art_archive.cover_art;
DROP TRIGGER IF EXISTS caa_delete ON musicbrainz.release;

CREATE TRIGGER caa_reindex AFTER UPDATE OR INSERT
    ON musicbrainz.release FOR EACH ROW
    EXECUTE PROCEDURE reindex_release();

CREATE TRIGGER caa_reindex AFTER UPDATE
    ON musicbrainz.artist FOR EACH ROW
    EXECUTE PROCEDURE reindex_artist();

CREATE TRIGGER caa_reindex AFTER UPDATE OR INSERT OR DELETE
    ON cover_art_archive.cover_art FOR EACH ROW
    EXECUTE PROCEDURE reindex_caa();

CREATE TRIGGER caa_reindex AFTER UPDATE OR INSERT OR DELETE
    ON cover_art_archive.cover_art_type FOR EACH ROW
    EXECUTE PROCEDURE reindex_caa_type();

CREATE TRIGGER caa_move BEFORE UPDATE
    ON cover_art_archive.cover_art FOR EACH ROW
    EXECUTE PROCEDURE move_release();

CREATE TRIGGER caa_delete BEFORE DELETE
    ON musicbrainz.release FOR EACH ROW
    EXECUTE PROCEDURE delete_release();

COMMIT;
