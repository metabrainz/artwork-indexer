\set ON_ERROR_STOP 1

BEGIN;

CREATE SCHEMA artwork_indexer;

SET search_path = artwork_indexer;

CREATE TYPE indexable_entity_type AS ENUM (
    'event',
    'release'
);

CREATE TYPE index_queue_action AS ENUM (
    'index',
    'move_image',
    'delete_image',
    'deindex'
);

CREATE TABLE index_queue (
    id                  BIGSERIAL,
    entity_type         indexable_entity_type NOT NULL,
    action              index_queue_action NOT NULL,
    message             JSONB NOT NULL,
    created             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    -- Note `index_queue_idx_uniq` below. Due to the requirement that
    -- events be unique, the `ON CONFLICT` action for external triggers
    -- should be `DO UPDATE SET attempts = 0` in order to revive dead
    -- events that have reached their maximum number of attempts. This
    -- means that `last_attempted` and `failure_reason` may be set from
    -- a previous failure even if `attempts` is 0.
    attempts            SMALLINT NOT NULL DEFAULT 0,
    last_attempted      TIMESTAMP WITH TIME ZONE,
    failure_reason      TEXT
);

ALTER TABLE index_queue
    ADD CONSTRAINT index_queue_okey
    PRIMARY KEY (id);

-- MusicBrainz Server will sometimes publish the same message multiple
-- times due to its SQL triggers firing for the same release (or event)
-- across multiple statements. It's therefore useful to enforce that
-- index events be unique.
CREATE UNIQUE INDEX index_queue_idx_uniq
    ON index_queue (entity_type, action, message);

COMMIT;
