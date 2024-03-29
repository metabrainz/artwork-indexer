-- Automatically generated, do not edit.

SET LOCAL client_min_messages = warning;

-- We drop the triggers first to simulate "CREATE OR REPLACE,"
-- which isn't implemented for "CREATE TRIGGER."

DROP TRIGGER IF EXISTS artwork_indexer_a_ins_cover_art ON cover_art_archive.cover_art;

CREATE TRIGGER artwork_indexer_a_ins_cover_art AFTER INSERT
    ON cover_art_archive.cover_art FOR EACH ROW
    EXECUTE PROCEDURE artwork_indexer.a_ins_cover_art();

DROP TRIGGER IF EXISTS artwork_indexer_b_upd_cover_art ON cover_art_archive.cover_art;

CREATE TRIGGER artwork_indexer_b_upd_cover_art BEFORE UPDATE
    ON cover_art_archive.cover_art FOR EACH ROW
    EXECUTE PROCEDURE artwork_indexer.b_upd_cover_art();

DROP TRIGGER IF EXISTS artwork_indexer_b_del_cover_art ON cover_art_archive.cover_art;

CREATE TRIGGER artwork_indexer_b_del_cover_art BEFORE DELETE
    ON cover_art_archive.cover_art FOR EACH ROW
    EXECUTE PROCEDURE artwork_indexer.b_del_cover_art();

DROP TRIGGER IF EXISTS artwork_indexer_a_ins_cover_art_type ON cover_art_archive.cover_art_type;

CREATE TRIGGER artwork_indexer_a_ins_cover_art_type AFTER INSERT
    ON cover_art_archive.cover_art_type FOR EACH ROW
    EXECUTE PROCEDURE artwork_indexer.a_ins_cover_art_type();

DROP TRIGGER IF EXISTS artwork_indexer_b_del_cover_art_type ON cover_art_archive.cover_art_type;

CREATE TRIGGER artwork_indexer_b_del_cover_art_type BEFORE DELETE
    ON cover_art_archive.cover_art_type FOR EACH ROW
    EXECUTE PROCEDURE artwork_indexer.b_del_cover_art_type();

DROP TRIGGER IF EXISTS artwork_indexer_b_del_release ON musicbrainz.release;

CREATE TRIGGER artwork_indexer_b_del_release BEFORE DELETE
    ON musicbrainz.release FOR EACH ROW
    EXECUTE PROCEDURE artwork_indexer.b_del_release();

DROP TRIGGER IF EXISTS artwork_indexer_a_upd_artist ON musicbrainz.artist;

CREATE TRIGGER artwork_indexer_a_upd_artist AFTER UPDATE
    ON musicbrainz.artist FOR EACH ROW
    EXECUTE PROCEDURE artwork_indexer.a_upd_artist();

DROP TRIGGER IF EXISTS artwork_indexer_a_upd_release ON musicbrainz.release;

CREATE TRIGGER artwork_indexer_a_upd_release AFTER UPDATE
    ON musicbrainz.release FOR EACH ROW
    EXECUTE PROCEDURE artwork_indexer.a_upd_release();

DROP TRIGGER IF EXISTS artwork_indexer_a_upd_release_meta ON musicbrainz.release_meta;

CREATE TRIGGER artwork_indexer_a_upd_release_meta AFTER UPDATE
    ON musicbrainz.release_meta FOR EACH ROW
    EXECUTE PROCEDURE artwork_indexer.a_upd_release_meta();

DROP TRIGGER IF EXISTS artwork_indexer_a_ins_release_first_release_date ON musicbrainz.release_first_release_date;

CREATE TRIGGER artwork_indexer_a_ins_release_first_release_date AFTER INSERT
    ON musicbrainz.release_first_release_date FOR EACH ROW
    EXECUTE PROCEDURE artwork_indexer.a_ins_release_first_release_date();

DROP TRIGGER IF EXISTS artwork_indexer_a_del_release_first_release_date ON musicbrainz.release_first_release_date;

CREATE TRIGGER artwork_indexer_a_del_release_first_release_date AFTER DELETE
    ON musicbrainz.release_first_release_date FOR EACH ROW
    EXECUTE PROCEDURE artwork_indexer.a_del_release_first_release_date();

