# artwork-indexer - update artwork index files at the Internet Archive
#
# Copyright (C) 2024  MetaBrainz Foundation
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

import json
import logging
import time

import psycopg


class PgConnWrapper(object):

    def __init__(self, config):
        self.config = config
        self.conn = None

    def connect(self):
        conninfo = psycopg.conninfo.make_conninfo(**self.config['database'])
        self.conn = psycopg.connect(
            conninfo,
            autocommit=True,
            row_factory=psycopg.rows.dict_row
        )

    def execute(self, query, params=None):
        if self.conn is None or self.conn.closed:
            self.connect()
        return self.conn.execute(query, params)

    def execute_with_retry(self, query, params=None):
        while True:
            try:
                return self.execute(query, params)
            except psycopg.OperationalError as exc:
                logging.error(exc)
                execute_args = ', '.join((
                    json.dumps(query),
                    json.dumps(params)
                ))
                logging.error(
                    'Command failed. Retrying in 30s: ' +
                    f'execute({execute_args})')
                time.sleep(30)

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()
            self.conn = None
