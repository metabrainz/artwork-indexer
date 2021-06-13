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
import re
import signal
import traceback
from collections import deque
from functools import partial
from math import inf
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


async def handle_event_failure(conn, event, error):
    logging.error(error)
    logging.error(''.join(traceback.format_tb(error.__traceback__)))

    # When an exception occurs, we only mark the event as failed
    # in two situations:
    #
    #   (1) it's reached MAX_ATTEMPTS
    #
    #   (2) an identical event with state = 'queued' exists
    #       (pushed while this one was running)
    #
    # Otherwise, the event stays queued and is retried later based
    # on the number of attempts so far. (See the `indexer` function
    # below for how this delay calculated.)
    #
    # 'failed' strictly means 'will not be retried'. An important
    # reason that we don't mark events as failed while they're waiting
    # to run again is the UNIQUE INDEX `event_queue_idx_queued_uniq`
    # only applies where state = 'queued'. We wouldn't want to allow
    # events to be inserted that duplicate ones that still have
    # attempts left; that would cause duplicate work at best, and
    # compounding failures at worst, not to mention bypass any delay in
    # processing we have on the existing event.

    await conn.execute(dedent('''
        UPDATE artwork_indexer.event_queue eq
        SET state = (
            CASE WHEN eq.attempts >= $1 OR EXISTS (
                SELECT 1
                FROM artwork_indexer.event_queue dup
                WHERE dup.state = 'queued'
                AND dup.action = eq.action
                AND dup.message = eq.message
                AND dup.id != $2
                FOR UPDATE
            ) THEN 'failed' ELSE 'queued' END
        )::artwork_indexer.event_state
        WHERE eq.id = $2
    '''), MAX_ATTEMPTS, event['id'])

    await conn.execute(dedent('''
        INSERT INTO artwork_indexer.event_failure_reason
            (event, failure_reason)
        VALUES ($1, $2)
    '''), event['id'], str(error))


async def complete_event(conn, event):
    await conn.execute(dedent('''
        UPDATE artwork_indexer.event_queue
        SET state = 'completed'
        WHERE id = $1
    '''), event['id'])


async def cleanup_events(pg_pool):
    # Cleanup completed events older than 90 days. We only keep these
    # around in case they help with debugging.
    #
    # Failed events are not cleaned up. These should always be
    # inspected and dealt with, not ignored and left for deletion.
    # (After all, it's less likely that they're due to transient server
    # issues given the number of times we retry them before marking
    # them as failed.)
    #
    # We clearly don't want to delete queued or running events, either.
    # It's so unlikely that an event would be in those states for 90
    # days that we'd *definitely* want to inspect them and find out
    # why.
    deletion_tag = await pg_pool.execute(dedent('''
        DELETE FROM artwork_indexer.event_queue
        WHERE state = 'completed'
        AND (now() - created) > interval '90 days'
    '''))
    deletion_count_match = re.match(r'DELETE ([0-9]+)', deletion_tag)
    if deletion_count_match:
        deletion_count = int(deletion_count_match[1])
        if deletion_count:
            logging.debug(
                'Deleted ' + str(deletion_count) + ' event' + \
                ('s' if deletion_count > 1 else '') + \
                ' older than 90 days')
            await pg_pool.execute(dedent('''
                SELECT setval(
                    pg_get_serial_sequence('artwork_indexer.event_queue', 'id'),
                    COALESCE((SELECT MAX(id) FROM artwork_indexer.event_queue), 0) + 1,
                    FALSE)
            '''))


async def run_event_handler(pg_pool, handler_method, message):
    async with \
        pg_pool.acquire() as pg_conn, \
        pg_conn.transaction():
        await handler_method(pg_conn, message)


async def indexer(config, maxwait, max_idle_loops=inf):
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
                asyncio.create_task(handle_event_failure(
                    pg_pool,
                    event,
                    task_exc,
                ))
            else:
                logging.debug(
                    'Event id=%s finished succesfully',
                    event['id'],
                )
                asyncio.create_task(complete_event(pg_pool, event))

        idle_loops = 0

        while True:
            await asyncio.sleep(sleep_amount)

            async with \
                pg_pool.acquire() as pg_conn, \
                pg_conn.transaction():

                # Skip events that have reached `MAX_ATTEMPTS`.
                # In other cases, `last_updated` should be within a
                # specific time interval. We start by waiting 30
                # minutes, and double the amount of time after each
                # attempt.
                event = await pg_conn.fetchrow(dedent('''
                    SELECT * FROM artwork_indexer.event_queue
                    WHERE state = 'queued'
                    AND attempts < $1
                    AND last_updated <=
                        (now() - (interval '30 minutes' * 2 * attempts))
                    ORDER BY created ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                '''), MAX_ATTEMPTS)

                # Reset `sleep_amount` if we're seeing activity, otherwise
                # increase it exponentially up to `maxwait` seconds.
                if event:
                    sleep_amount = 1
                    idle_loops = 0
                else:
                    # Since there's nothing else to do, cleanup old events.
                    asyncio.create_task(cleanup_events(pg_pool))

                    idle_loops += 1
                    if idle_loops >= max_idle_loops:
                        break

                    if sleep_amount < maxwait:
                        sleep_amount = min(sleep_amount * 2, maxwait)
                        logging.debug('No event found; sleeping for %s second(s)', sleep_amount)

                    continue

                await pg_conn.execute(dedent('''
                    UPDATE artwork_indexer.event_queue
                    SET state = 'running',
                        attempts = attempts + 1
                    WHERE id = $1
                '''), event['id'])

                logging.info('Processing event %s', event)

                try:
                    parsed_message = json.loads(event['message'])
                except BaseException as e:
                    await handle_event_failure(pg_conn, event, e)

                handler = event_handler_map[event['entity_type']]

                task = asyncio.create_task(run_event_handler(
                    pg_pool,
                    getattr(handler, event['action']),
                    parsed_message,
                ))
                task.add_done_callback(partial(task_done, event))


def main():
    arg_parser = argparse.ArgumentParser(
        description='update artwork index files at the Internet Archive',
    )
    arg_parser.add_argument('--config',
                            help='path to config file',
                            dest='config',
                            type=str,
                            default='config.ini')
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

    config.read(args.config)
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
