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

  * Python >= 3.11
  * PostgreSQL >= 12

You will need a MusicBrainz database. See the `INSTALL.md` document of the
[musicbrainz-server](https://github.com/metabrainz/musicbrainz-server)
project for more details.

## Installation

  1. Create a [virtual environment](https://docs.python.org/3/library/venv.html)
     and activate it. For example:

      ```sh
      python3 -m venv .venv
      . .venv/bin/activate.fish  # I use fish shell
      ```

  2. Install dependencies:
       ```sh
       pip install -r requirements.txt
       ```

  3. Copy `config.default.ini` to `config.ini` and edit appropriately.

  4. Run `indexer.py`:
       ```sh
       python indexer.py
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

Failed events (any that encounter an exception during their execution) are
tried up to 5 times; only after all attempts have been exhausted is an
event's `state` set to `failed`. Failed events are never cleaned up and must
be monitored and dealt with manually. All errors are logged to the
`artwork_indexer.event_failure_reason` table (in addition to stderr and
Sentry, if the latter is configured).

Succesful events (marked as `completed`) are kept for 90 days before they
are cleaned up.
