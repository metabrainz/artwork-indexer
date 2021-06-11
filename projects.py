# artwork-indexer - update artwork index files at the Internet Archive
#
# Copyright (C) 2021  MetaBrainz Foundation
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


# CAA (release) mapping from *_mb_metadata.xml to *_meta.xml at the IA:
#
# /ns:metadata/ns:release/ns:title
#      => title
#
# /ns:metadata/ns:release/ns:artist-credit/ns:name-credit/ns:artist/ns:name
#      => creator
#
# /ns:metadata/ns:release/ns:artist-credit/ns:name-credit/ns:artist/ns:sort-name
#      => creator-alt-script
#
# /ns:metadata/ns:release/ns:date
#      => date
#
# /ns:metadata/ns:release/ns:text-representation/ns:language
#      => language
#
# /ns:metadata/ns:release/@id
#      => array('external-identifier', 'urn:mb_release_id:{value}')
#
# /ns:metadata/ns:release/ns:artist-credit/ns:name-credit/ns:artist/@id
#      => array('external-identifier', 'urn:mb_artist_id:{value}')
#
# /ns:metadata/ns:release/ns:barcode
#      => array('external-identifier', 'urn:upc:{value}')
#
# /ns:metadata/ns:release/ns:asin'
#      => array('external-identifier', 'urn:asin:{value}')

CAA_PROJECT = {
    'abbr': 'caa',
    'art_schema': 'cover_art_archive',
    'art_table': 'cover_art',
    'entity_schema': 'musicbrainz',
    'entity_table': 'release',
    'domain': 'coverartarchive.org',
    'ia_collection': 'coverartarchive',
    'ws_inc_params': 'artists',
    'indexed_metadata': (
        {
            'schema': 'musicbrainz',
            'table': 'artist',
            'indexed_columns': (
                {
                    'name': 'name',
                    'nullable': False,
                },
                {
                    'name': 'sort_name',
                    'nullable': False,
                },
            ),
            'joins': (
                {
                    'lhs': (
                        'musicbrainz',
                        'artist_credit_name',
                        'artist_credit',
                    ),
                    'rhs': ('musicbrainz', 'release', 'artist_credit'),
                },
            ),
            'condition':
                'musicbrainz.artist_credit_name.artist = {tg_rowvar}.id',
            'tg_ops': ('upd',),
        },
        {
            'schema': 'musicbrainz',
            'table': 'release',
            'indexed_columns': (
                {
                    'name': 'name',
                    'nullable': False,
                },
                {
                    'name': 'artist_credit',
                    'nullable': False,
                },
                {
                    'name': 'language',
                    'nullable': True,
                },
                {
                    'name': 'barcode',
                    'nullable': True,
                },
            ),
            'tg_ops': ('upd',),
        },
        {
            'schema': 'musicbrainz',
            'table': 'release_meta',
            'indexed_columns': (
                {
                    'name': 'amazon_asin',
                    'nullable': True,
                },
            ),
            'condition': 'musicbrainz.release.id = {tg_rowvar}.id',
            'tg_ops': ('upd',),
        },
        {
            'schema': 'musicbrainz',
            'table': 'release_first_release_date',
            'columns': (),
            'condition': 'musicbrainz.release.id = {tg_rowvar}.release',
            'tg_ops': ('ins', 'del'),
        },
    ),
}

EAA_PROJECT = {
    'abbr': 'eaa',
    'art_schema': 'event_art_archive',
    'art_table': 'event_art',
    'entity_schema': 'musicbrainz',
    'entity_table': 'event',
    'domain': 'eventartarchive.org',
    'ia_collection': 'eventartarchive',
    'ws_inc_params': 'artist-rels+place-rels',
    'indexed_metadata': (
        {
            'schema': 'musicbrainz',
            'table': 'event',
            'indexed_columns': (
                {
                    'name': 'name',
                    'nullable': False,
                },
            ),
            'tg_ops': ('upd',),
        },
    ),
}

PROJECTS = (
    CAA_PROJECT,
    EAA_PROJECT,
)
