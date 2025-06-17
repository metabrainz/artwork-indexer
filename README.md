# artwork-indexer

A daemon that watches the `artwork_indexer.event_queue` table for events,
and updates relevant
[index.json files and other metadata](https://archive.org/download/mbid-59105e60-a6f7-4a86-aaab-2c4f02ddb4f8)
at the Internet Archive.

We support indexing
[releases](https://wiki.musicbrainz.org/Release) for the
[Cover Art Archive](http://coverartarchive.org) and
[events](https://wiki.musicbrainz.org/Event) for the Event Art Archive.

Succesor to the old, Perl-and-RabbitMQ-based
[CAA-indexer](https://github.com/metabrainz/CAA-indexer).

## Requirements

  * Python >= 3.13
  * [Poetry](https://python-poetry.org/) >= 1.8.0
  * PostgreSQL >= 12

You will need a MusicBrainz database. See the `INSTALL.md` document of the
[musicbrainz-server](https://github.com/metabrainz/musicbrainz-server)
project for more details.

## Installation

  1. Install project dependencies with `poetry`:

      ```sh
      poetry install
      ```

  3. Copy `config.default.ini` to `config.ini` and edit appropriately.

  4. Install the `artwork_indexer` schema, plus associated functions and
     triggers. (This will use the database configured in `config.ini`.)

       ```sh
       poetry run python indexer.py --setup-schema
       ```

  5. Run `indexer.py`:
       ```sh
       poetry run python indexer.py
       ```

## Testing

Tests are executed via [run_tests.sh](run_tests.sh). You may have to first
configure the environment variables `DROPDB_COMMAND` and
`POSTGRES_SUPERUSER`; the defaults are specified below. (It's not necessary
to include these if the defaults match your system.)

```sh
env DROPDB_COMMAND='sudo -u postgres dropdb' \
    POSTGRES_SUPERUSER=postgres \
    ./run_tests.sh
```

On macOS I use the following configuration:

```sh
env DROPDB_COMMAND=dropdb \
    POSTGRES_SUPERUSER=michael \
    ./run_tests.sh
```

## Maintenance

### Reindexing an entity

If the index.json for a particular entity is corrupted or out-of-date and needs to be regenerated, you can trigger an index event from psql:

```sh
$ ssh jimmy
bitmap@jimmy:~$ docker exec -it postgres-jimmy psql -U musicbrainz musicbrainz_db
musicbrainz_db=> INSERT INTO artwork_indexer.event_queue (entity_type, action, message)
                      VALUES ('release', 'index',
                              jsonb_build_object('gid', 'e02c28af-8f42-4ea4-928c-4c5244b7c10a'));
INSERT 0 1
```

### Inspecting failures

To retrieve all current failed events, run:

```sh
musicbrainz_db=> SELECT q.*, string_agg(fr.failure_reason, E'\n')
                   FROM artwork_indexer.event_queue q
                   JOIN artwork_indexer.event_failure_reason fr ON fr.event = q.id
                  WHERE q.state = 'failed'
               GROUP BY q.id
               ORDER BY q.last_updated DESC;
```

These must be cleaned up by hand once they're no longer needed (i.e., after
the underlying issue is determined and resolved).

## Hacking

There are two primary tasks of the artwork-indexer:

 1. Determine when metadata at the Internet Archive needs to be updated, and
    process events to update it. There are two types of metadata files:

    * `index.json`: Contains information about the available images for an
      entity; is exposed through the CAA or EAA API.

    * `*_mb_metadata.xml`: Contains MusicBrainz web service XML for an
      entity; used by the IA to display information about an entity on its
      `/details/` page. The `inc` parameters we use depend on what kind of
      information the IA displays.

 2. Move images between buckets when releases are merged, and delete images
    (plus associated metadata) when releases are deleted.

These tasks are accomplished via database triggers, contained in the
[sql](sql/) subdirectory. Since these triggers (and the functions they call)
are very similar between the CAA and EAA, we don't keep them in sync by hand,
but have a script to compile them: [generate_code.py](generate_code.py).

The triggers push indexer events to the `artwork_indexer.event_queue` table.
(You can find the full `artwork_indexer` schema in
[create_schema.sql](sql/create_schema.sql).) The main
[indexer.py](indexer.py) script polls this table for new events and executes
the appropriate handlers for them (see [handlers.py](handlers.py) and
[handlers_base.py](handlers_base.py)).

Event types and their expected `message` format are documented below.

| event type    | message format                                                          | description                                                             |
| ------------- | ----------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| index         | `{"gid": UUID}`                                                         | uploads index.json and MB metadata to the IA                            |
| copy_image    | `{"artwork_id": INT, "old_gid": UUID, "new_gid": UUID, "suffix": TEXT}` | copies an image from one bucket to another (after a release is merged)  |
| delete_image  | `{"gid": UUID, "artwork_id": INT, "suffix": TEXT}`                      | deletes an image (including after a release is merged or deleted)       |
| deindex       | `{"gid": UUID}`                                                         | deletes index.json (after a release is deleted)                         |
| noop          | `{}` or `{"fail": BOOL}` or `{"sleep": REAL}`                           | for testing/debugging (does nothing, or optionally fails or sleeps)     |

Failed events (any that encounter an exception during their execution) are
tried up to 5 times; only after all attempts have been exhausted is an
event's `state` set to `failed`. Failed events are never cleaned up and must
be monitored and dealt with manually. All errors are logged to the
`artwork_indexer.event_failure_reason` table (in addition to stderr and
Sentry, if the latter is configured).

Succesful events (marked as `completed`) are kept for 90 days before they
are cleaned up.
