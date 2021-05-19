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
    -- means that `last_attempted` and `failure_reason` may be set from
    -- a previous failure even if `attempts` is 0.
    attempts            SMALLINT NOT NULL DEFAULT 0,
    last_attempted      TIMESTAMP WITH TIME ZONE,
    failure_reason      TEXT
);

ALTER TABLE event_queue
    ADD CONSTRAINT event_queue_okey
    PRIMARY KEY (id);

-- MusicBrainz Server will sometimes publish the same message multiple
-- times due to its SQL triggers firing for the same release (or event)
-- across multiple statements. It's therefore useful to enforce that
-- index events be unique.
CREATE UNIQUE INDEX event_queue_idx_uniq
    ON event_queue (entity_type, action, message);

COMMIT;
