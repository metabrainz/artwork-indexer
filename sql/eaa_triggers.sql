-- Automatically generated, do not edit.

\set ON_ERROR_STOP 1

BEGIN;

SET LOCAL search_path = 'event_art_archive';
SET LOCAL client_min_messages = warning;

-- We drop the triggers first to simulate "CREATE OR REPLACE,"
-- which isn't implemented for "CREATE TRIGGER."

DROP TRIGGER IF EXISTS a_ins_event_art_eaa ON event_art_archive.event_art;

CREATE TRIGGER a_ins_event_art_eaa AFTER INSERT
    ON event_art_archive.event_art FOR EACH ROW
    EXECUTE PROCEDURE event_art_archive.a_ins_event_art();

DROP TRIGGER IF EXISTS a_upd_event_art_eaa ON event_art_archive.event_art;

CREATE TRIGGER a_upd_event_art_eaa AFTER UPDATE
    ON event_art_archive.event_art FOR EACH ROW
    EXECUTE PROCEDURE event_art_archive.a_upd_event_art();

DROP TRIGGER IF EXISTS a_del_event_art_eaa ON event_art_archive.event_art;

CREATE TRIGGER a_del_event_art_eaa AFTER DELETE
    ON event_art_archive.event_art FOR EACH ROW
    EXECUTE PROCEDURE event_art_archive.a_del_event_art();

DROP TRIGGER IF EXISTS a_ins_event_art_type_eaa ON event_art_archive.event_art_type;

CREATE TRIGGER a_ins_event_art_type_eaa AFTER INSERT
    ON event_art_archive.event_art_type FOR EACH ROW
    EXECUTE PROCEDURE event_art_archive.a_ins_event_art_type();

DROP TRIGGER IF EXISTS a_del_event_art_type_eaa ON event_art_archive.event_art_type;

CREATE TRIGGER a_del_event_art_type_eaa AFTER DELETE
    ON event_art_archive.event_art_type FOR EACH ROW
    EXECUTE PROCEDURE event_art_archive.a_del_event_art_type();

DROP TRIGGER IF EXISTS a_del_event_eaa ON musicbrainz.event;

CREATE TRIGGER a_del_event_eaa AFTER DELETE
    ON musicbrainz.event FOR EACH ROW
    EXECUTE PROCEDURE event_art_archive.a_del_event();

DROP TRIGGER IF EXISTS a_upd_event_eaa ON musicbrainz.event;

CREATE TRIGGER a_upd_event_eaa AFTER UPDATE
    ON musicbrainz.event FOR EACH ROW
    EXECUTE PROCEDURE event_art_archive.a_upd_event();

COMMIT;
