# artwork-indexer - update CAA index files at the Internet Archive
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

import logging
import json
import time

from psycopg import sql
from requests.exceptions import HTTPError
from textwrap import dedent
import urllib.parse


IMAGE_FILE_FORMAT = '{bucket}-{id}.{suffix}'

# connect, read timeouts
REQUEST_TIMEOUT = (10, 30)


def kebab(s):
    return s.replace('_', '-')


class EventHandler:

    image_url_format = 'https://{domain}/{subpath}/{gid}/{id}{size}.{suffix}'

    def __init__(self,
                 config,
                 http_session):
        self.config = config
        self.http_session = http_session

    @property
    def artwork_schema(self):
        raise NotImplementedError

    @property
    def domain(self):
        raise NotImplementedError

    @property
    def entity_type(self):
        raise NotImplementedError

    # Acts as a bucket & image filename prefix.
    @property
    def gid_name(self):
        raise NotImplementedError

    @property
    def ia_collection(self):
        raise NotImplementedError

    @property
    def project_abbr(self):
        raise NotImplementedError

    def build_authorization_header(self):
        abbr = self.project_abbr
        s3_conf = self.config['s3']
        return {
            'authorization': 'LOW %s:%s' % (
                s3_conf[abbr + '_access'],
                s3_conf[abbr + '_secret'],
            ),
        }

    def build_bucket_name(self, gid):
        return '{gid_name}-{gid}'.format(
            gid_name=self.gid_name,
            gid=gid,
        )

    def build_canonical_entity_url(self, gid):
        raise NotImplementedError

    def build_image_json(self, gid, row):
        raise NotImplementedError

    def build_image_url(self, gid, artwork_id, size, suffix):
        return EventHandler.image_url_format.format(
            domain=self.domain,
            subpath=self.entity_type,
            gid=gid,
            id=artwork_id,
            size=(('-%s' % size) if size is not None else ''),
            suffix=suffix,
        )

    def build_metadata_ia_filename(self, gid):
        raise NotImplementedError

    def build_metadata_url(self, gid):
        raise NotImplementedError

    def build_metadata_headers(self):
        raise NotImplementedError

    def build_s3_item_url(self, gid, filename):
        url = self.config['s3']['url']
        bucket = self.build_bucket_name(gid)
        return url.format(bucket=bucket, file=filename)

    def fetch_image_rows(self, pg_conn, entity_gid):
        raise NotImplementedError

    def index(self, pg_conn, event):
        message = event['message']
        gid = message['gid']

        index_json_content = json.dumps({
            'images': [
                self.build_image_json(gid, row)
                for row in self.fetch_image_rows(pg_conn, gid)
            ],
            kebab(self.entity_type): self.build_canonical_entity_url(gid),
        }, sort_keys=True)

        logging.debug('Produced %s', index_json_content)

        index_json_upload_url = self.build_s3_item_url(gid, 'index.json')
        try:
            index_json_upload_res = self.http_session.put(
                index_json_upload_url,
                data=index_json_content.encode('utf-8'),
                headers={
                    **self.build_authorization_header(),
                    'content-type': 'application/json; charset=UTF-8',
                    'x-archive-auto-make-bucket': '1',
                    'x-archive-keep-old-version': '1',
                    'x-archive-meta-collection': self.ia_collection,
                    'x-archive-meta-mediatype': 'image',
                    'x-archive-meta-noindex': 'true',
                },
                timeout=REQUEST_TIMEOUT
            )
            index_json_upload_res.raise_for_status()
        except HTTPError as exc:
            logging.info('Upload of %s failed', index_json_upload_url)
            logging.error('Response text: %s', index_json_upload_res.text)
            raise exc

        logging.info('Upload of %s succeeded', index_json_upload_url)

        entity_metadata_url = self.build_metadata_url(gid)
        entity_metadata_headers = self.build_metadata_headers()
        try:
            entity_metadata_res = self.http_session.get(
                entity_metadata_url,
                headers=entity_metadata_headers,
                stream=True,
                timeout=REQUEST_TIMEOUT
            )
            entity_metadata_res.raise_for_status()
        except HTTPError as exc:
            logging.info('Fetch of %s failed', entity_metadata_url)
            logging.error('Response text: %s', entity_metadata_res.text)
            raise exc

        entity_metadata_upload_url = self.build_s3_item_url(
            gid,
            self.build_metadata_ia_filename(gid),
        )
        try:
            entity_metadata_upload_res = self.http_session.put(
                entity_metadata_upload_url,
                data=entity_metadata_res.content,
                headers={
                    **self.build_authorization_header(),
                    'content-type': 'application/xml; charset=UTF-8',
                    'x-archive-auto-make-bucket': '1',
                    'x-archive-meta-collection': self.ia_collection,
                    'x-archive-meta-mediatype': 'image',
                    'x-archive-meta-noindex': 'true',
                },
                timeout=REQUEST_TIMEOUT
            )
            entity_metadata_upload_res.raise_for_status()
        except HTTPError as exc:
            logging.info('Upload of %s failed', entity_metadata_upload_url)
            logging.error('Response text: %s', entity_metadata_upload_res.text)
            raise exc

        logging.info('Upload of %s succeeded', entity_metadata_upload_url)

    def copy_image(self, pg_conn, event):
        message = event['message']
        artwork_id = message['artwork_id']
        old_gid = message['old_gid']
        new_gid = message['new_gid']
        suffix = message['suffix']

        old_bucket = self.build_bucket_name(old_gid)
        new_bucket = self.build_bucket_name(new_gid)

        old_file_name = IMAGE_FILE_FORMAT.format(
            bucket=old_bucket,
            id=artwork_id,
            suffix=suffix
        )
        new_file_name = IMAGE_FILE_FORMAT.format(
            bucket=new_bucket,
            id=artwork_id,
            suffix=suffix
        )
        source_file_path = '/{bucket}/{file}'.format(
            bucket=old_bucket,
            file=old_file_name,
        )
        target_url = self.build_s3_item_url(new_gid, new_file_name)

        # Copy the image to the new MBID. (The old image will be deleted by a
        # subsequent and dependant `delete_image` event.)
        try:
            copy_res = self.http_session.put(
                target_url,
                headers={
                    **self.build_authorization_header(),
                    'x-amz-copy-source': source_file_path,
                    'x-archive-auto-make-bucket': '1',
                    'x-archive-keep-old-version': '1',
                    'x-archive-meta-collection': self.ia_collection,
                    'x-archive-meta-mediatype': 'image',
                    'x-archive-meta-noindex': 'true',
                },
                timeout=REQUEST_TIMEOUT
            )
            copy_res.raise_for_status()
        except HTTPError as exc:
            logging.info('Copy from %s to %s failed',
                         source_file_path, target_url)
            logging.error('Response text: %s', copy_res.text)
            raise exc

        logging.info('Copy from %s to %s succeeded',
                     source_file_path, target_url)

    def delete_image(self, pg_conn, event):
        message = event['message']
        gid = message['gid']

        if event['depends_on'] is None:
            # If a `delete_image` event exists with no parent,
            # there should be no later `copy_image` event for
            # the same image. Verify this to be safe.
            later_copy_image_event = pg_conn.execute(dedent('''
                SELECT id FROM artwork_indexer.event_queue eq
                WHERE eq.state = 'queued'
                AND eq.action = 'copy_image'
                AND eq.created > %(created)s
                AND eq.message->'artwork_id' = %(artwork_id)s
                AND eq.message->'old_gid' = %(gid)s
                AND eq.message->'suffix' = %(suffix)s
                LIMIT 1
            '''), {
                'created': event['created'],
                'artwork_id': json.dumps(message['artwork_id']),
                'gid': json.dumps(gid),
                'suffix': json.dumps(message['suffix']),
            }).fetchone()

            if later_copy_image_event:
                latest_copy_image_event_id = later_copy_image_event['id']
                raise Exception(
                    'This image cannot be deleted, because ' +
                    'a later event exists ' +
                    f'(id={latest_copy_image_event_id}) ' +
                    'that wants to copy it.'
                )

        filename = IMAGE_FILE_FORMAT.format(
            bucket=self.build_bucket_name(gid),
            id=message['artwork_id'],
            suffix=message['suffix']
        )
        target_url = self.build_s3_item_url(gid, filename)

        # Note: This request should succeed (204) even if the file
        # no longer exists.
        try:
            delete_res = self.http_session.delete(
                target_url,
                headers={
                    **self.build_authorization_header(),
                    'x-archive-keep-old-version': '1',
                    'x-archive-cascade-delete': '1',
                },
                timeout=REQUEST_TIMEOUT
            )
            delete_res.raise_for_status()
        except HTTPError as exc:
            logging.info('Deletion of %s failed', target_url)
            logging.error('Response text: %s', delete_res.text)
            raise exc

        logging.info('Deletion of %s succeeded', target_url)

    def deindex(self, pg_conn, event):
        message = event['message']
        gid = message['gid']

        target_url = self.build_s3_item_url(gid, 'index.json')

        # Note: This request should succeed (204) even if the file
        # no longer exists.
        try:
            deindex_res = self.http_session.delete(
                target_url,
                headers={
                    **self.build_authorization_header(),
                    'x-archive-keep-old-version': '1',
                    'x-archive-cascade-delete': '1',
                },
                timeout=REQUEST_TIMEOUT
            )
            deindex_res.raise_for_status()
        except HTTPError as exc:
            logging.info('Deletion of %s failed', target_url)
            logging.error('Response text: %s', deindex_res.text)
            raise exc

        logging.info('Deletion of %s succeeded', target_url)

    def noop(self, pg_conn, event):
        message = event['message']
        if message.get('fail'):
            raise Exception('Failure (no-op)')
        if 'sleep' in message:
            time.sleep(message['sleep'])


class MusicBrainzEventHandler(EventHandler):

    @property
    def gid_name(self):
        return 'mbid'

    @property
    def ws_inc_params(self):
        raise NotImplementedError

    def build_canonical_entity_url(self, gid):
        return 'https://musicbrainz.org/{entity}/{gid}'.format(
            entity=kebab(self.entity_type),
            gid=gid,
        )

    def build_image_json(self, mbid, row):
        artwork_id = row['id']
        image_json = {
            'types': row['types'],
            'front': bool(row['is_front']),
            'comment': row['comment'],
            'image': self.build_image_url(
                mbid, artwork_id, None, row['suffix']),
            'thumbnails': {
                'small': self.build_image_url(mbid, artwork_id, 250, 'jpg'),
                'large': self.build_image_url(mbid, artwork_id, 500, 'jpg'),
                '250': self.build_image_url(mbid, artwork_id, 250, 'jpg'),
                '500': self.build_image_url(mbid, artwork_id, 500, 'jpg'),
                '1200': self.build_image_url(mbid, artwork_id, 1200, 'jpg'),
            },
            'approved': bool(row['approved']),
            'edit': row['edit'],
            'id': row['id'],
        }
        if 'is_back' in row:
            image_json['back'] = bool(row['is_back'])
        return image_json

    def build_metadata_ia_filename(self, gid):
        return self.build_bucket_name(gid) + '_mb_metadata.xml'

    def build_metadata_url(self, gid):
        mb_url = urllib.parse.urlparse(self.config['musicbrainz']['url'])
        xmlws_path = '/ws/2/{entity}/{gid}?inc={inc}'.format(
            entity=kebab(self.entity_type),
            gid=gid,
            inc=self.ws_inc_params,
        )
        return urllib.parse.urlunparse(mb_url._replace(path=xmlws_path))

    def build_metadata_headers(self):
        headers = {}
        database = self.config['musicbrainz'].get('database')
        if database:
            headers['mb-set-database'] = database
        return headers

    def fetch_image_rows(self, pg_conn, mbid):
        schema = self.artwork_schema
        entity_type = self.entity_type

        # Note: cover_art_archive.image_type is also used by the
        # event_art_archive schema.
        return pg_conn.execute(
            sql.SQL(dedent('''
                SELECT * FROM {schema}.index_listing
                JOIN cover_art_archive.image_type USING (mime_type)
                WHERE {entity} = (SELECT id FROM {entity} WHERE gid = %(gid)s)
                ORDER BY ordering
            ''')).format(
                schema=sql.Identifier(schema),
                entity=sql.Identifier(entity_type),
            ),
            {'gid': mbid},
        ).fetchall()
