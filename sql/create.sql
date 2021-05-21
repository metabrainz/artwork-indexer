\set ON_ERROR_STOP 1

BEGIN;

CREATE SCHEMA artwork_indexer;

SET search_path = artwork_indexer;

CREATE TYPE indexable_entity_type AS ENUM (
    'event', -- MusicBrainz event, not to be confused with indexer events
    'release'
);

CREATE TYPE event_queue_action AS ENUM (
    'index',
    'move_image',
    'delete_image',
    'deindex'
);

CREATE TABLE event_queue (
    id                  BIGSERIAL,
    entity_type         indexable_entity_type NOT NULL,
    action              event_queue_action NOT NULL,
    message             JSONB NOT NULL,
    created             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    -- Note `event_queue_idx_uniq` below. Due to the requirement that
    -- events be unique, the `ON CONFLICT` action for external triggers
    -- should be `DO UPDATE SET attempts = 0` in order to revive dead
    -- events that have reached their maximum number of attempts. This
    -- means that `last_attempted` may be set from a previous failure
    -- even if `attempts` is 0.
    attempts            SMALLINT NOT NULL DEFAULT 0,
    last_attempted      TIMESTAMP WITH TIME ZONE,
    last_updated        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE event_failure_reason (
    event               BIGINT NOT NULL,
    failure_reason      TEXT NOT NULL,
    created             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

ALTER TABLE event_queue
    ADD CONSTRAINT event_queue_pkey
    PRIMARY KEY (id);

ALTER TABLE event_failure_reason
    ADD CONSTRAINT event_failure_reason_fk_event
    FOREIGN KEY (event)
    REFERENCES event_queue(id)
    ON DELETE CASCADE;

-- MusicBrainz Server will sometimes publish the same message multiple
-- times due to its SQL triggers firing for the same release (or event)
-- across multiple statements. It's therefore useful to enforce that
-- index events be unique.
CREATE UNIQUE INDEX event_queue_idx_uniq
    ON event_queue (entity_type, action, message);

CREATE INDEX event_failure_reason_idx_event
    ON event_failure_reason (event, created);

CREATE OR REPLACE FUNCTION b_upd_event_queue()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.attempts > OLD.attempts THEN
        NEW.last_attempted = NOW();
    END IF;
    NEW.last_updated = NOW();
    RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';

CREATE TRIGGER b_upd_event_queue
    BEFORE UPDATE ON event_queue
    FOR EACH ROW EXECUTE FUNCTION b_upd_event_queue();

COMMIT;
