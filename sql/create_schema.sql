CREATE SCHEMA artwork_indexer;

CREATE TYPE artwork_indexer.indexable_entity_type AS ENUM (
    'event', -- MusicBrainz event, not to be confused with indexer events
    'release'
);

CREATE TYPE artwork_indexer.event_queue_action AS ENUM (
    'index',
    'copy_image',
    'delete_image',
    'deindex',
    'noop'
);

CREATE TYPE artwork_indexer.event_state AS ENUM (
    -- 'queued' events are waiting to run, and are generally not
    -- blocked from doing so when it's their turn (based on order of
    -- creation), unless their parent (depends_on) has 'failed', in
    -- which case they're stuck until the failed parent event is dealt
    -- with.
    --
    -- There cannot be more than one queued event for the same
    -- (entity_type, action, message) tuple. See
    -- `event_queue_idx_queued_uniq` below.
    'queued',
    -- 'running' events have started processing and are currently
    -- being handled by the indexer process.  Events generally must
    -- perform synchronous database queries and HTTP requests
    -- so may take some time to complete.
    --
    -- If a running event encounters an error, and the value of the
    -- `attempts` column is less than MAX_ATTEMPTS, we simply reset
    -- the event as `queued` (though the indexer will apply an
    -- increasing delay based on the number of attempts before it's
    -- retried).  This is in addition to logging the error in the
    -- `event_failure_reason` table.
    'running',
    -- 'failed' events have reached MAX_ATTEMPTS and will not be
    -- retried.  Due to the increasing delays after each attempt, it's
    -- unlikely to be a transient issue, so they must be inspected
    -- by an admin and re-queued manually once the underlying issue
    -- is addressed, or deleted as appropriate.
    'failed',
    -- 'completed' events are as they're named, but kept around for
    -- debugging purposes for 90 days.
    'completed'
);

CREATE TABLE artwork_indexer.event_queue (
    id                  BIGSERIAL,
    state               artwork_indexer.event_state NOT NULL DEFAULT 'queued',
    entity_type         artwork_indexer.indexable_entity_type NOT NULL,
    action              artwork_indexer.event_queue_action NOT NULL,
    message             JSONB NOT NULL,
    depends_on          BIGINT[],
    created             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    -- Note `event_queue_idx_queued_uniq` below. Due to the requirement
    -- that queued events be unique, external triggers should have an
    -- `ON CONFLICT` action.
    attempts            SMALLINT NOT NULL DEFAULT 0,
    last_updated        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE artwork_indexer.event_failure_reason (
    event               BIGINT NOT NULL,
    failure_reason      TEXT NOT NULL,
    created             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

ALTER TABLE artwork_indexer.event_queue
    ADD CONSTRAINT event_queue_pkey
    PRIMARY KEY (id);

ALTER TABLE artwork_indexer.event_failure_reason
    ADD CONSTRAINT event_failure_reason_fk_event
    FOREIGN KEY (event)
    REFERENCES artwork_indexer.event_queue(id)
    ON DELETE CASCADE;

--- MusicBrainz Server will sometimes publish the same message multiple
--- times due to its SQL triggers firing for the same release (or event)
--- across multiple statements. It's therefore useful to enforce that
--- queued index events be unique.
CREATE UNIQUE INDEX event_queue_idx_queued_uniq
    ON artwork_indexer.event_queue (entity_type, action, message)
    WHERE state = 'queued';

CREATE INDEX event_queue_idx_state_created
    ON artwork_indexer.event_queue (state, created);

CREATE INDEX event_failure_reason_idx_event
    ON artwork_indexer.event_failure_reason (event, created);

CREATE OR REPLACE FUNCTION artwork_indexer.b_upd_event_queue()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.last_updated = NEW.last_updated THEN
        NEW.last_updated = NOW();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';

CREATE TRIGGER b_upd_event_queue
    BEFORE UPDATE ON artwork_indexer.event_queue
    FOR EACH ROW EXECUTE FUNCTION artwork_indexer.b_upd_event_queue();
