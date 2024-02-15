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

import json
import urllib.parse
from textwrap import dedent


IMAGE_FILE_FORMAT = '{bucket}-{id}.{suffix}'


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

    def build_authorization_header(self):
        s3_conf = self.config['s3']
        return {
            'authorization': 'LOW %s:%s' % (
                s3_conf['access'],
                s3_conf['secret'],
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

    def index(self, pg_conn, message):
        gid = message['gid']

        encoded_index_json = json.dumps({
            'images': [
                self.build_image_json(gid, row)
                for row in self.fetch_image_rows(pg_conn, gid)
            ],
            kebab(self.entity_type): self.build_canonical_entity_url(gid),
        }, ensure_ascii=True, sort_keys=True).encode('ascii')

        self.http_session.put(
            self.build_s3_item_url(gid, 'index.json'),
            data=encoded_index_json,
            headers={
                **self.build_authorization_header(),
                'content-type': 'application/json; charset=US-ASCII',
                'x-archive-auto-make-bucket': '1',
                'x-archive-keep-old-version': '1',
                'x-archive-meta-collection': self.ia_collection,
                'x-archive-meta-mediatype': 'image',
                'x-archive-meta-noindex': 'true',
            },
        ).raise_for_status()

        entity_metadata_url = self.build_metadata_url(gid)
        entity_metadata_headers = self.build_metadata_headers()
        metadata_res = self.http_session.get(entity_metadata_url,
                                             headers=entity_metadata_headers,
                                             stream=True)
        metadata_res.raise_for_status()
        self.http_session.put(
            self.build_s3_item_url(
                gid,
                self.build_metadata_ia_filename(gid),
            ),
            data=metadata_res.content,
            headers={
                **self.build_authorization_header(),
                'content-type': 'application/xml; charset=UTF-8',
                'x-archive-auto-make-bucket': '1',
                'x-archive-meta-collection': self.ia_collection,
                'x-archive-meta-mediatype': 'image',
                'x-archive-meta-noindex': 'true',
            },
        ).raise_for_status()

    def copy_image(self, pg_conn, message):
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

        # Copy the image to the new MBID, and delete the old image.
        self.http_session.put(
            self.build_s3_item_url(new_gid, new_file_name),
            headers={
                **self.build_authorization_header(),
                'x-amz-copy-source': source_file_path,
                'x-archive-auto-make-bucket': '1',
                'x-archive-keep-old-version': '1',
                'x-archive-meta-collection': self.ia_collection,
                'x-archive-meta-mediatype': 'image',
                'x-archive-meta-noindex': 'true',
            },
        ).raise_for_status()

    def delete_image(self, pg_conn, message):
        gid = message['gid']

        filename = IMAGE_FILE_FORMAT.format(
            bucket=self.build_bucket_name(gid),
            id=message['artwork_id'],
            suffix=message['suffix']
        )
        # Note: This request should succeed (204) even if the file
        # no longer exists.
        self.http_session.delete(
            self.build_s3_item_url(gid, filename),
            headers={
                **self.build_authorization_header(),
                'x-archive-keep-old-version': '1',
                'x-archive-cascade-delete': '1',
            },
        ).raise_for_status()

    def deindex(self, pg_conn, message):
        # Note: This request should succeed (204) even if the file
        # no longer exists.
        self.http_session.delete(
            self.build_s3_item_url(message['gid'], 'index.json'),
            headers={
                **self.build_authorization_header(),
                'x-archive-keep-old-version': '1',
                'x-archive-cascade-delete': '1',
            },
        ).raise_for_status()

    def noop(self, pg_conn, message):
        if message.get('fail'):
            raise Exception('Failure (no-op)')


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
            dedent('''
                SELECT * FROM {schema}.index_listing
                JOIN cover_art_archive.image_type USING (mime_type)
                WHERE {entity} = (SELECT id FROM {entity} WHERE gid = %(gid)s)
                ORDER BY ordering
            '''.format(schema=schema, entity=entity_type)),
            {'gid': mbid},
        ).fetchall()
