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
import configparser
import datetime
import logging
import os
import signal
import sys
import time
import traceback
from math import inf
from textwrap import dedent

import requests
import sentry_sdk

from handlers import EVENT_HANDLER_CLASSES
from pg_conn_wrapper import PgConnWrapper

# Maximum number of times we should try to handle an event
# before we give up. This works together with the `attempts`
# column in the `artwork_indexer.event_queue` table. Events
# that reach this number of attempts will be skipped for
# processing and require manual intervention; start by
# inspecting the `event_failure_reason` table.
MAX_ATTEMPTS = 5

# When set to True, indicates to the `indexer` event loop that it should
# stop once idle.
SHUTDOWN_SIGNAL = False


def handle_event_failure(pg_conn, event, error):
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
    # the number of attempts so far. (See the `get_next_event` function
    # below for how this delay calculated.)
    #
    # Identical 'queued' events are blocked at the database level by a
    # UNIQUE INDEX, `event_queue_idx_queued_uniq`. This prevents
    # duplicate work from being queued, and is why we don't mark events
    # as 'failed' while they're waiting to be retried: the 'failed'
    # state would allow a new, identical event to be queued while the
    # failed event still has attempts left. Besides duplicating work,
    # this would cause compounding failures at worst, and bypass any
    # delay in processing we have on the existing event.

    pg_conn.execute_with_retry(dedent('''
        UPDATE artwork_indexer.event_queue eq
        SET state = (
            CASE WHEN eq.attempts >= %(max_attempts)s OR EXISTS (
                SELECT 1
                FROM artwork_indexer.event_queue dup
                WHERE dup.state = 'queued'
                AND dup.action = eq.action
                AND dup.message = eq.message
                AND dup.id != %(event_id)s
                FOR UPDATE
            ) THEN 'failed' ELSE 'queued' END
        )::artwork_indexer.event_state
        WHERE eq.id = %(event_id)s
    '''), {
        'max_attempts': MAX_ATTEMPTS,
        'event_id': event['id'],
    })

    pg_conn.execute_with_retry(dedent('''
        INSERT INTO artwork_indexer.event_failure_reason
            (event, failure_reason)
        VALUES (%(event_id)s, %(error)s)
    '''), {'event_id': event['id'], 'error': str(error)})

    try:
        sentry_sdk.capture_exception(error)
    except BaseException as sentry_exc:
        logging.error(sentry_exc)


def cleanup_events(pg_conn):
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
    pg_cur = pg_conn.execute(dedent('''
        DELETE FROM artwork_indexer.event_queue
        WHERE state = 'completed'
        AND (now() - created) > interval '90 days'
    '''))
    pg_conn.commit()
    if pg_cur.rowcount:
        logging.info(
            'Deleted ' + str(pg_cur.rowcount) + ' event' +
            ('s' if pg_cur.rowcount > 1 else '') +
            ' older than 90 days')

    # Additionally, mark events that have been running for more than
    # 2.5 minutes as failed.
    timed_out_events = pg_conn.execute(dedent('''
        UPDATE artwork_indexer.event_queue
        SET state = 'failed'
        WHERE state = 'running'
        AND (last_updated - created) > interval '2.5 minutes'
        RETURNING id, (last_updated - created) AS duration
    ''')).fetchall()
    for event in timed_out_events:
        seconds_elapsed = event['duration'].total_seconds()
        pg_conn.execute(dedent('''
            INSERT INTO artwork_indexer.event_failure_reason
                (event, failure_reason)
            VALUES (%(event_id)s, %(error)s)
        '''), {
            'event_id': event['id'],
            'error': 'This event was marked as failed because ' +
                     'it had been running for more than 2.5 minutes ' +
                     f'({seconds_elapsed} seconds).'
        })
    pg_conn.commit()


def get_next_event(pg_conn):
    # Skip events that have reached `MAX_ATTEMPTS`.
    # In other cases, `last_updated` should be within a
    # specific time interval. We start by waiting 1 hour,
    # and wait an additional hour per each attempt.
    return pg_conn.execute(dedent('''
        SELECT * FROM artwork_indexer.event_queue eq
        WHERE eq.state = 'queued'
        AND eq.attempts < %(max_attempts)s
        AND eq.last_updated <=
            (now() - (interval '1 hour' * eq.attempts))
        AND (eq.depends_on IS NULL OR NOT EXISTS (
            SELECT TRUE
            FROM artwork_indexer.event_queue parent_eq
            WHERE parent_eq.id = any(eq.depends_on)
            AND parent_eq.state != 'completed'
        ))
        ORDER BY created, id
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    '''), {'max_attempts': MAX_ATTEMPTS}).fetchone()


def run_event_handler(pg_conn, event, handler):
    handler_method = getattr(handler, event['action'])
    try:
        handler_method(pg_conn, event)
    except BaseException as task_exc:
        handle_event_failure(pg_conn, event, task_exc)
    else:
        logging.info(
            'Event id=%s completed succesfully',
            event['id'],
        )
        pg_conn.execute_with_retry(dedent('''
            UPDATE artwork_indexer.event_queue
            SET state = 'completed'
            WHERE id = %(event_id)s
        '''), {'event_id': event['id']})


def indexer(
    config,
    pg_conn,
    maxwait,
    max_idle_loops=inf,
    http_client_cls=requests.Session
):
    sleep_amount = 1  # seconds

    http_session = http_client_cls()
    http_session.headers.update({
        'user-agent': 'metabrainz/artwork-indexer ' +
                      f'({requests.utils.default_user_agent()})',
    })

    event_handler_map = {
        entity: cls(config, http_session)
        for entity, cls in EVENT_HANDLER_CLASSES.items()
    }

    idle_loops = 0
    last_cleanup_datetime = datetime.datetime.min

    while not SHUTDOWN_SIGNAL:
        time.sleep(sleep_amount)

        event = get_next_event(pg_conn)

        # Reset `sleep_amount` if we're seeing activity, otherwise
        # increase it exponentially up to `maxwait` seconds.
        if event:
            sleep_amount = 1
            idle_loops = 0
        else:
            # Close the transaction opened by `get_next_event`.
            pg_conn.commit()

            # Since there's nothing else to do, cleanup old events
            # (but not too often).
            current_datetime = datetime.datetime.now()
            seconds_elapsed_since_last_cleanup = \
                (current_datetime - last_cleanup_datetime).total_seconds()
            if seconds_elapsed_since_last_cleanup >= 150:
                cleanup_events(pg_conn)
                last_cleanup_datetime = current_datetime

            idle_loops += 1
            if idle_loops >= max_idle_loops:
                break

            if sleep_amount < maxwait:
                sleep_amount = min(sleep_amount * 2, maxwait)
                logging.info(
                    'No event found; sleeping for %s second(s)',
                    sleep_amount,
                )

            continue

        if event['state'] != 'queued':
            # Close the transaction opened by `get_next_event`.
            pg_conn.commit()

            # This is mainly a development aid.  In at least one
            # occasion I broke the SQL query above by having bad
            # boolean operator precedence.  -- mwiencek
            raise Exception('Event is not queued: %r', event)

        # XXX: This is used by tests/test_concurrency.sh to check
        # that concurrent indexer processes don't select the same
        # queued events. A small delay is added before we mark them
        # as `running`.
        time.sleep(0.25)

        logging.info('Processing event %s', event)

        pg_conn.execute_and_commit(dedent('''
            UPDATE artwork_indexer.event_queue
            SET state = 'running',
                attempts = attempts + 1
            WHERE id = %(event_id)s
        '''), {'event_id': event['id']})

        handler = event_handler_map[event['entity_type']]

        run_event_handler(
            pg_conn,
            event,
            handler,
        )
        pg_conn.commit()

    pg_conn.close()


def setup_schema(pg_conn):
    curdir = os.path.dirname(__file__)

    schema_exists = pg_conn.execute(dedent('''
        SELECT TRUE
          FROM information_schema.schemata
         WHERE schema_name = 'artwork_indexer';
    ''')).fetchone()
    if schema_exists:
        logging.error(
            'ERROR: The `artwork_indexer` schema already exists. ' +
            'If you would like to recreate it, drop it first.',
        )
        sys.exit(1)

    for file_name in (
        'create_schema',
        'caa_functions',
        'eaa_functions',
        'caa_triggers',
        'eaa_triggers',
    ):
        with open(os.path.join(curdir, 'sql', file_name + '.sql')) as fp:
            pg_conn.execute(fp.read())

    pg_conn.commit()


def main():
    arg_parser = argparse.ArgumentParser(
        description='update artwork index files at the Internet Archive',
    )
    arg_parser.add_argument('--config',
                            help='path to config file',
                            dest='config',
                            type=str,
                            default='config.ini')
    arg_parser.add_argument('--max-wait',
                            help='max poll timeout',
                            dest='maxwait',
                            type=int,
                            default=32)
    arg_parser.add_argument('--max-idle-loops',
                            help='exit after this many idle loops',
                            dest='max_idle_loops',
                            type=int,
                            default=inf)
    arg_parser.add_argument('--setup-schema',
                            help='install the schema and exit',
                            dest='setup_schema',
                            action='store_true')
    args = arg_parser.parse_args()

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    config = configparser.ConfigParser()
    config.read(args.config)

    pg_conn = PgConnWrapper(config)

    if args.setup_schema:
        setup_schema(pg_conn)
        sys.exit(0)

    def reload_configuration(signum, frame):
        logging.info('Got SIGHUP, reloading configuration')
        config.read('config.ini')

    signal.signal(signal.SIGHUP, reload_configuration)

    def shutdown(signum, frame):
        logging.info(f'Got signal {signum}, shutting down')
        global SHUTDOWN_SIGNAL
        SHUTDOWN_SIGNAL = True

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    if 'sentry' in config:
        sentry_dsn = config['sentry'].get('dsn')
        if sentry_dsn:
            sentry_sdk.init(dsn=sentry_dsn)

    indexer(config, pg_conn, args.maxwait,
            max_idle_loops=args.max_idle_loops)


if __name__ == '__main__':
    main()
