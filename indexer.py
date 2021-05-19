# artwork-indexer - update artwork index files at the Internet Archive
#
# Copyright (C) 2020  MetaBrainz Foundation
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

import argparse
import asyncio
import configparser
import json
import logging
import signal
import traceback
from collections import deque
from functools import partial
from textwrap import dedent

import aiohttp
import asyncpg

from handlers import (
    EventEventHandler,
    ReleaseEventHandler,
)

# Maximum number of times we should try to handle an event
# before we give up. This works together with the `attempts`
# column in the `artwork_indexer.event_queue` table. Events
# that reach this number of attempts will be skipped for
# processing and require manual intervention; start by
# inspecting the `event_failure_reason` table.
MAX_ATTEMPTS = 5

EVENT_HANDLER_CLASSES = {
    'event': EventEventHandler, # MusicBrainz event
    'release': ReleaseEventHandler, # MusicBrainz release
}


async def log_failure_reason(conn, event, error):
    logging.error(error)
    logging.error(''.join(traceback.format_tb(error.__traceback__)))

    await conn.execute(dedent('''
        INSERT INTO artwork_indexer.event_failure_reason
            (event, failure_reason)
        VALUES ($1, $2)
    '''), event['id'], str(error))


async def delete_event(conn, event):
    logging.debug('Deleting event id=%s', event['id'])

    await conn.execute(dedent('''
        DELETE FROM artwork_indexer.event_queue
        WHERE id = $1
    '''), event['id'])


async def run_event_handler(pg_pool, handler_method, message):
    async with \
        pg_pool.acquire() as pg_conn, \
        pg_conn.transaction():
        await handler_method(pg_conn, message)


async def indexer(config, maxwait):
    sleep_amount = 1 # seconds

    async with \
        asyncpg.create_pool(**config['database']) as pg_pool, \
        aiohttp.ClientSession(raise_for_status=True) as http_session:

        event_handler_map = {
            entity: cls(config, http_session)
            for entity, cls in EVENT_HANDLER_CLASSES.items()
        }

        def task_done(event, task):
            task_exc = task.exception()
            if task_exc:
                asyncio.create_task(log_failure_reason(
                    pg_pool,
                    event,
                    task_exc,
                ))
            else:
                logging.debug(
                    'Event id=%s finished succesfully',
                    event['id'],
                )
                asyncio.create_task(delete_event(pg_pool, event))

        while True:
            logging.debug('Sleeping for %s second(s)', sleep_amount)

            await asyncio.sleep(sleep_amount)

            async with \
                pg_pool.acquire() as pg_conn, \
                pg_conn.transaction():

                # Skip events that have reached `MAX_ATTEMPTS`.
                # In other cases, `last_attempted` should either be
                # NULL before the first attempt, or within a specific
                # time interval. We start by waiting 30 minutes, and
                # double the amount of time after each attempt.
                event = await pg_conn.fetchrow(dedent('''
                    SELECT * FROM artwork_indexer.event_queue
                    WHERE attempts < $1
                    AND (
                        last_attempted IS NULL
                        OR last_attempted <=
                            (now() - (interval '30 minutes' * 2 * attempts))
                    )
                    ORDER BY id ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                '''), MAX_ATTEMPTS)

                # Reset `sleep_amount` if we're seeing activity, otherwise
                # increase it exponentially up to `maxwait` seconds.
                if event:
                    sleep_amount = 1
                else:
                    if sleep_amount < maxwait:
                        sleep_amount = min(sleep_amount * 2, maxwait)
                    continue

                logging.info('Processing event %s', event)

                await pg_conn.execute(dedent('''
                    UPDATE artwork_indexer.event_queue
                    SET attempts = attempts + 1,
                        last_attempted = now()
                    WHERE id = $1
                '''), event['id'])

                handler = event_handler_map[event['entity_type']]

                try:
                    message = json.loads(event['message'])
                except BaseException as e:
                    await log_failure_reason(pg_conn, event, e)

                task = asyncio.create_task(run_event_handler(
                    pg_pool,
                    getattr(handler, event['action']),
                    message,
                ))
                task.add_done_callback(partial(task_done, event))


def main():
    arg_parser = argparse.ArgumentParser(
        description='update artwork index files at the Internet Archive',
    )
    arg_parser.add_argument('--debug',
                            help='enable debug mode',
                            dest='debug',
                            action='store_true')
    arg_parser.add_argument('--max-wait',
                            help='max poll timeout',
                            dest='maxwait',
                            type=int,
                            default=32)
    args = arg_parser.parse_args()

    logger = logging.getLogger()
    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    config = configparser.ConfigParser()

    def reload_configuration(signum, frame):
        logging.info('Got SIGHUP, reloading configuration')
        config.read('config.ini')

    config.read('config.ini')
    signal.signal(signal.SIGHUP, reload_configuration)

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(indexer(config, args.maxwait))
    except KeyboardInterrupt:
        pass
    finally:
        loop.stop()
        loop.close()


if __name__ == '__main__':
    main()
