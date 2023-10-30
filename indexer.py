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
from math import inf
from textwrap import dedent

import aiohttp
import asyncpg

from handlers import EVENT_HANDLER_CLASSES

# Maximum number of times we should try to handle an event
# before we give up. This works together with the `attempts`
# column in the `artwork_indexer.event_queue` table. Events
# that reach this number of attempts will be skipped for
# processing and require manual intervention; start by
# inspecting the `event_failure_reason` table.
MAX_ATTEMPTS = 5


async def handle_event_failure(pg_conn, event, error):
    logging.error(error)
    logging.error(''.join(traceback.format_tb(error.__traceback__)))

    # The 'failed' state strictly means 'will not be retried'. When an
    # exception occurs, we only mark the event as failed in two
    # situations:
    #
    #   (1) it's reached MAX_ATTEMPTS
    #
    #   (2) an identical event with state = 'queued' exists (pushed
    #       while this one was running)
    #
    # Otherwise, the event stays queued and is retried later based on
    # the number of attempts so far. (See the `indexer` function below
    # for how this delay calculated.)
    #
    # Identical 'queued' events are blocked at the database level by a
    # UNIQUE INDEX, `event_queue_idx_queued_uniq`. This prevents
    # duplicate work from being queued, and is why we don't mark events
    # as 'failed' while they're waiting to be retried: the 'failed'
    # state would allow a new, identical event to be queued while the
    # failed event still has attempts left. Besides duplicating work,
    # this would cause compounding failures at worst, and bypass any
    # delay in processing we have on the existing event.

    await pg_conn.execute(dedent('''
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

    await pg_conn.execute(dedent('''
        INSERT INTO artwork_indexer.event_failure_reason
            (event, failure_reason)
        VALUES ($1, $2)
    '''), event['id'], str(error))


async def cleanup_events(pg_pool):
    # Cleanup completed events older than 90 days. We only keep these
    # around in case they help with debugging.
    #
    # Failed events are not cleaned up. These should always be inspected
    # and dealt with, not ignored and left for deletion. (It's less
    # likely they're due to transient server issues, because we retry
    # them a number of times before marking them as failed.)
    #
    # We don't want to delete queued or running events that are older
    # than 90 days: if this occurs, we'd want to inspect them to find
    # out why they're stuck (ideally before 90 days has passed).
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
                'Deleted ' + str(deletion_count) + ' event' +
                ('s' if deletion_count > 1 else '') +
                ' older than 90 days')


async def run_event_handler(pg_pool, event, handler, message):
    async with pg_pool.acquire() as pg_conn, pg_conn.transaction():
        handler_method = getattr(handler, event['action'])
        try:
            await handler_method(pg_conn, message)
        except BaseException as task_exc:
            await handle_event_failure(pg_conn, event, task_exc)
        else:
            logging.debug(
                'Event id=%s finished succesfully',
                event['id'],
            )
            await pg_conn.execute(dedent('''
                UPDATE artwork_indexer.event_queue
                SET state = 'completed'
                WHERE id = $1
            '''), event['id'])


async def indexer(
    config,
    maxwait,
    max_idle_loops=inf,
    http_client_cls=aiohttp.ClientSession
):
    sleep_amount = 1  # seconds

    async with \
            asyncpg.create_pool(**config['database']) as pg_pool, \
            http_client_cls(raise_for_status=True) as http_session:
        event_handler_map = {
            entity: cls(config, http_session)
            for entity, cls in EVENT_HANDLER_CLASSES.items()
        }

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
                    SELECT * FROM artwork_indexer.event_queue eq
                    WHERE eq.state = 'queued'
                    AND eq.attempts < $1
                    AND eq.last_updated <=
                        (now() - (interval '30 minutes' * 2 * eq.attempts))
                    AND (eq.depends_on IS NULL OR EXISTS (
                        SELECT TRUE
                        FROM artwork_indexer.event_queue parent_eq
                        WHERE array_position(
                            eq.depends_on,
                            parent_eq.id
                        ) IS NOT NULL
                        AND parent_eq.state = 'completed'
                    ))
                    ORDER BY created, id
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
                        logging.debug(
                            'No event found; sleeping for %s second(s)',
                            sleep_amount,
                        )

                    continue

                if event['state'] != 'queued':
                    # This is mainly a development aid.  In at least one
                    # occasion I broke the SQL query above by having bad
                    # boolean operator precedence.  -- mwiencek
                    raise Exception('Event is not queued: %r', event)

                logging.info('Processing event %s', event)

                try:
                    parsed_message = json.loads(event['message'])
                except BaseException as e:
                    await handle_event_failure(pg_conn, event, e)

                if event['action'] == 'delete_image' and \
                        event['depends_on'] is None:
                    # If `delete_image` event exists with no parent,
                    # there should be no later `copy_image` event for
                    # the same image. Verify this to be safe.
                    later_copy_image_event = await pg_conn.fetchrow(dedent('''
                        SELECT id FROM artwork_indexer.event_queue eq
                        WHERE eq.state = 'queued'
                        AND eq.action = 'copy_image'
                        AND eq.created > $1
                        AND eq.message->'artwork_id' = $2
                        AND eq.message->'old_gid' = $3
                        AND eq.message->'suffix' = $4
                        LIMIT 1
                    '''), event['created'], *[
                        json.dumps(x) for x in (
                            parsed_message['artwork_id'],
                            parsed_message['gid'],
                            parsed_message['suffix'],
                        )
                    ])

                    if later_copy_image_event:
                        await handle_event_failure(
                            pg_conn,
                            event,
                            Exception(
                                'This image cannot be deleted, because ' +
                                'a later event exists ' +
                                f'(id={later_copy_image_event}) ' +
                                'that wants to copy it.'
                            )
                        )
                        continue

                await pg_conn.execute(dedent('''
                    UPDATE artwork_indexer.event_queue
                    SET state = 'running',
                        attempts = attempts + 1
                    WHERE id = $1
                '''), event['id'])

                handler = event_handler_map[event['entity_type']]

                asyncio.create_task(run_event_handler(
                    pg_pool,
                    event,
                    handler,
                    parsed_message,
                ))


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

    loop = None
    try:
        loop = asyncio.run(indexer(config, args.maxwait))
    except KeyboardInterrupt:
        pass
    finally:
        if loop:
            loop.stop()
            loop.close()


if __name__ == '__main__':
    main()
