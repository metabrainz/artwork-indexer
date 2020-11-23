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

import io
import json
import logging
import urllib.parse
from textwrap import dedent


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

    def build_s3_item_url(self, gid, filename):
        url = self.config['s3']['url']
        bucket = self.build_bucket_name(gid)
        return url.format(bucket=bucket, file=filename)

    async def fetch_entity_row(self, pg_conn, gid):
        raise NotImplementedError

    async def fetch_image_rows(self, pg_conn, entity_row):
        raise NotImplementedError

    async def index(self, pg_conn, message):
        given_gid = message['gid']

        entity = await self.fetch_entity_row(pg_conn, given_gid)
        if entity is None:
            logging.debug(
                '%s %s does not exist, skipping indexing',
                self.entity_type,
                given_gid,
            )
            return

        gid = entity['gid'] # in case of redirect

        encoded_index_json = json.dumps({
            'images': [
                self.build_image_json(gid, row)
                for row in await self.fetch_image_rows(pg_conn, entity)
            ],
            kebab(self.entity_type): self.build_canonical_entity_url(gid),
        }, sort_keys=True)

        await self.http_session.put(
            self.build_s3_item_url(gid, 'index.json'),
            data=io.StringIO(encoded_index_json),
            headers={
                **self.build_authorization_header(),
                'x-archive-auto-make-bucket': '1',
                'x-archive-keep-old-version': '1',
                'x-archive-meta-collection': self.ia_collection,
            },
        )

        entity_metadata_url = self.build_metadata_url(gid)

        async with self.http_session.get(entity_metadata_url) as response:
            entity_metadata = await response.text()
            await self.http_session.put(
                self.build_s3_item_url(
                    gid,
                    self.build_metadata_ia_filename(gid),
                ),
                data=io.StringIO(entity_metadata),
                headers={
                    **self.build_authorization_header(),
                    'x-archive-auto-make-bucket': '1',
                    'x-archive-meta-collection': self.ia_collection,
                },
            )

    async def move_image(self, pg_conn, message):
        artwork_id = message['artwork_id']
        old_gid = message['old_gid']
        new_gid = message['new_gid']
        suffix = message['suffix']

        image_file_format = '{bucket}-{id}.{suffix}'
        old_bucket = self.build_bucket_name(old_gid)
        new_bucket = self.build_bucket_name(new_gid)

        old_file_name = image_file_format.format(
            bucket=old_bucket,
            id=artwork_id,
            suffix=suffix
        )
        new_file_name = image_file_format.format(
            bucket=new_bucket,
            id=artwork_id,
            suffix=suffix
        )
        source_file_path = '/{bucket}/{file}'.format(
            bucket=old_bucket,
            file=old_file_name,
        )

        # Copy the image to the new MBID, and delete the old image.
        await self.http_session.put(
            self.build_s3_item_url(new_gid, new_file_name),
            headers={
                **self.build_authorization_header(),
                'x-amz-copy-source': source_file_path,
                'x-archive-auto-make-bucket': '1',
                'x-archive-keep-old-version': '1',
                'x-archive-meta-collection': self.ia_collection,
                'x-archive-meta-mediatype': 'images',
            },
        )
        await self.http_session.delete(
            self.build_s3_item_url(old_gid, old_file_name),
            headers={
                **self.build_authorization_header(),
                'x-archive-keep-old-version': '1',
            },
        )

    async def delete_image(self, pg_conn, message):
        gid = message['gid']

        filename = IMAGE_FILE_FORMAT.format(
            mbid=gid,
            id=message['artwork_id'],
            suffix=message['suffix']
        )
        await self.http_session.delete(
            self.build_s3_item_url(gid, filename),
            headers={
                **self.build_authorization_header(),
                'x-archive-keep-old-version': '1',
            },
        )

    async def deindex(self, pg_conn, message):
        await self.http_session.delete(
            self.build_s3_item_url(message['gid'], 'index.json'),
            headers={
                **self.build_authorization_header(),
                'x-archive-keep-old-version': '1',
            },
        )


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
        return {
            'types': row['types'],
            'front': bool(row['is_front']),
            'comment': row['comment'],
            'image': self.build_image_url(mbid, artwork_id, None, row['suffix']),
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

    async def fetch_entity_row(self, pg_conn, mbid):
        entity_type = self.entity_type

        return await pg_conn.fetchrow(dedent('''
            SELECT id, gid
            FROM musicbrainz.{entity}
            WHERE id IN (
                SELECT new_id
                FROM musicbrainz.{entity}_gid_redirect
                WHERE gid = $1
                UNION ALL
                SELECT id
                FROM musicbrainz.{entity}
                WHERE gid = $1
            )
        '''.format(entity=entity_type)), mbid)

    async def fetch_image_rows(self, pg_conn, entity_row):
        schema = self.artwork_schema
        entity_type = self.entity_type

        return await pg_conn.fetch(
            dedent('''
                SELECT * FROM {schema}.index_listing
                JOIN {schema}.image_type USING (mime_type)
                WHERE {entity} = $1
                ORDER BY ordering
            '''.format(schema=schema, entity=entity_type),
        ), entity_row['id'])


class ReleaseEventHandler(MusicBrainzEventHandler):

    @property
    def artwork_schema(self):
        return 'cover_art_archive'

    @property
    def domain(self):
        return 'coverartarchive.org'

    @property
    def entity_type(self):
        return 'release'

    @property
    def ia_collection(self):
        return 'coverartarchive'

    @property
    def ws_inc_params(self):
        return 'artists'

    def build_image_json(self, mbid, row):
        image_json = super().build_image_json(mbid, row)
        image_json['back'] = bool(row['is_back'])
        return image_json


class EventEventHandler(MusicBrainzEventHandler):

    @property
    def artwork_schema(self):
        return 'event_art_archive'

    @property
    def domain(self):
        return 'eventartarchive.org'

    @property
    def entity_type(self):
        return 'event'

    @property
    def ia_collection(self):
        return 'eventartarchive'

    @property
    def ws_inc_params(self):
        return 'artist-rels+place-rels'
