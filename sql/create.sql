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

CREATE TYPE event_state AS ENUM (
    'queued',
    'running',
    'failed',
    'completed'
);

CREATE TABLE event_queue (
    id                  SERIAL,
    state               event_state NOT NULL DEFAULT 'queued',
    entity_type         indexable_entity_type NOT NULL,
    action              event_queue_action NOT NULL,
    message             JSONB NOT NULL,
    created             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    -- Note `event_queue_idx_queued_uniq` below. Due to the requirement
    -- that queued events be unique, external triggers should have an
    -- `ON CONFLICT DO NOTHING` action.
    attempts            SMALLINT NOT NULL DEFAULT 0,
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

--- MusicBrainz Server will sometimes publish the same message multiple
--- times due to its SQL triggers firing for the same release (or event)
--- across multiple statements. It's therefore useful to enforce that
--- queued index events be unique.
CREATE UNIQUE INDEX event_queue_idx_queued_uniq
    ON event_queue (entity_type, action, message)
    WHERE state = 'queued';

CREATE INDEX event_queue_idx_state_created
    ON event_queue (state, created);

CREATE INDEX event_failure_reason_idx_event
    ON event_failure_reason (event, created);

CREATE OR REPLACE FUNCTION b_upd_event_queue()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.last_updated = NEW.last_updated THEN
        NEW.last_updated = NOW();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';

CREATE TRIGGER b_upd_event_queue
    BEFORE UPDATE ON event_queue
    FOR EACH ROW EXECUTE FUNCTION b_upd_event_queue();

COMMIT;
