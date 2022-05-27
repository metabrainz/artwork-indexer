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

  * Python >= 3.9
  * PostgreSQL >= 12
  * aiohttp
  * asyncpg

You will need a MusicBrainz database. See the `INSTALL.md` document of the
[musicbrainz-server](https://github.com/metabrainz/musicbrainz-server)
project for more details.

## Installation

  1. Create a [virtual environment](https://docs.python.org/3/library/venv.html)
     and activate it.

  2. Install dependencies:
       ```sh
       pip install -r requirements.txt
       ```

  3. Copy `config.default.ini` to `config.ini` and edit appropriately.

  4. Run `indexer.py`:
       ```sh
       python indexer.py
       ```
