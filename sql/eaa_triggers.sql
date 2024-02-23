-- Automatically generated, do not edit.

SET LOCAL client_min_messages = warning;

-- We drop the triggers first to simulate "CREATE OR REPLACE,"
-- which isn't implemented for "CREATE TRIGGER."

DROP TRIGGER IF EXISTS artwork_indexer_a_ins_event_art ON event_art_archive.event_art;

CREATE TRIGGER artwork_indexer_a_ins_event_art AFTER INSERT
    ON event_art_archive.event_art FOR EACH ROW
    EXECUTE PROCEDURE artwork_indexer.a_ins_event_art();

DROP TRIGGER IF EXISTS artwork_indexer_a_upd_event_art ON event_art_archive.event_art;

CREATE TRIGGER artwork_indexer_a_upd_event_art AFTER UPDATE
    ON event_art_archive.event_art FOR EACH ROW
    EXECUTE PROCEDURE artwork_indexer.a_upd_event_art();

DROP TRIGGER IF EXISTS artwork_indexer_a_del_event_art ON event_art_archive.event_art;

CREATE TRIGGER artwork_indexer_a_del_event_art AFTER DELETE
    ON event_art_archive.event_art FOR EACH ROW
    EXECUTE PROCEDURE artwork_indexer.a_del_event_art();

DROP TRIGGER IF EXISTS artwork_indexer_a_ins_event_art_type ON event_art_archive.event_art_type;

CREATE TRIGGER artwork_indexer_a_ins_event_art_type AFTER INSERT
    ON event_art_archive.event_art_type FOR EACH ROW
    EXECUTE PROCEDURE artwork_indexer.a_ins_event_art_type();

DROP TRIGGER IF EXISTS artwork_indexer_a_del_event_art_type ON event_art_archive.event_art_type;

CREATE TRIGGER artwork_indexer_a_del_event_art_type AFTER DELETE
    ON event_art_archive.event_art_type FOR EACH ROW
    EXECUTE PROCEDURE artwork_indexer.a_del_event_art_type();

DROP TRIGGER IF EXISTS artwork_indexer_a_del_event ON musicbrainz.event;

CREATE TRIGGER artwork_indexer_a_del_event AFTER DELETE
    ON musicbrainz.event FOR EACH ROW
    EXECUTE PROCEDURE artwork_indexer.a_del_event();

DROP TRIGGER IF EXISTS artwork_indexer_a_upd_event ON musicbrainz.event;

CREATE TRIGGER artwork_indexer_a_upd_event AFTER UPDATE
    ON musicbrainz.event FOR EACH ROW
    EXECUTE PROCEDURE artwork_indexer.a_upd_event();

