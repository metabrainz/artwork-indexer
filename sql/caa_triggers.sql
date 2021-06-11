-- Automatically generated, do not edit.

\set ON_ERROR_STOP 1

BEGIN;

SET LOCAL search_path = 'cover_art_archive';
SET LOCAL client_min_messages = warning;

-- We drop the triggers first to simulate "CREATE OR REPLACE,"
-- which isn't implemented for "CREATE TRIGGER."

DROP TRIGGER IF EXISTS a_ins_cover_art_caa ON cover_art_archive.cover_art;

CREATE TRIGGER a_ins_cover_art_caa AFTER INSERT
    ON cover_art_archive.cover_art FOR EACH ROW
    EXECUTE PROCEDURE cover_art_archive.a_ins_cover_art();

DROP TRIGGER IF EXISTS a_upd_cover_art_caa ON cover_art_archive.cover_art;

CREATE TRIGGER a_upd_cover_art_caa AFTER UPDATE
    ON cover_art_archive.cover_art FOR EACH ROW
    EXECUTE PROCEDURE cover_art_archive.a_upd_cover_art();

DROP TRIGGER IF EXISTS a_del_cover_art_caa ON cover_art_archive.cover_art;

CREATE TRIGGER a_del_cover_art_caa AFTER DELETE
    ON cover_art_archive.cover_art FOR EACH ROW
    EXECUTE PROCEDURE cover_art_archive.a_del_cover_art();

DROP TRIGGER IF EXISTS a_ins_cover_art_type_caa ON cover_art_archive.cover_art_type;

CREATE TRIGGER a_ins_cover_art_type_caa AFTER INSERT
    ON cover_art_archive.cover_art_type FOR EACH ROW
    EXECUTE PROCEDURE cover_art_archive.a_ins_cover_art_type();

DROP TRIGGER IF EXISTS a_del_cover_art_type_caa ON cover_art_archive.cover_art_type;

CREATE TRIGGER a_del_cover_art_type_caa AFTER DELETE
    ON cover_art_archive.cover_art_type FOR EACH ROW
    EXECUTE PROCEDURE cover_art_archive.a_del_cover_art_type();

DROP TRIGGER IF EXISTS a_del_release_caa ON musicbrainz.release;

CREATE TRIGGER a_del_release_caa AFTER DELETE
    ON musicbrainz.release FOR EACH ROW
    EXECUTE PROCEDURE cover_art_archive.a_del_release();

DROP TRIGGER IF EXISTS a_upd_artist_caa ON musicbrainz.artist;

CREATE TRIGGER a_upd_artist_caa AFTER UPDATE
    ON musicbrainz.artist FOR EACH ROW
    EXECUTE PROCEDURE cover_art_archive.a_upd_artist();

DROP TRIGGER IF EXISTS a_upd_release_caa ON musicbrainz.release;

CREATE TRIGGER a_upd_release_caa AFTER UPDATE
    ON musicbrainz.release FOR EACH ROW
    EXECUTE PROCEDURE cover_art_archive.a_upd_release();

DROP TRIGGER IF EXISTS a_upd_release_meta_caa ON musicbrainz.release_meta;

CREATE TRIGGER a_upd_release_meta_caa AFTER UPDATE
    ON musicbrainz.release_meta FOR EACH ROW
    EXECUTE PROCEDURE cover_art_archive.a_upd_release_meta();

DROP TRIGGER IF EXISTS a_ins_release_first_release_date_caa ON musicbrainz.release_first_release_date;

CREATE TRIGGER a_ins_release_first_release_date_caa AFTER INSERT
    ON musicbrainz.release_first_release_date FOR EACH ROW
    EXECUTE PROCEDURE cover_art_archive.a_ins_release_first_release_date();

DROP TRIGGER IF EXISTS a_del_release_first_release_date_caa ON musicbrainz.release_first_release_date;

CREATE TRIGGER a_del_release_first_release_date_caa AFTER DELETE
    ON musicbrainz.release_first_release_date FOR EACH ROW
    EXECUTE PROCEDURE cover_art_archive.a_del_release_first_release_date();

COMMIT;
