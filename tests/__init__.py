import asyncpg
import configparser
import json
import unittest
from aiohttp import ClientSession
from contextlib import asynccontextmanager
from urllib.parse import urlparse
from textwrap import dedent


tests_config = configparser.ConfigParser()
tests_config.read('config.tests.ini')


MBS_TEST_URL = tests_config['musicbrainz']['url']
MBS_TEST_NETLOC = urlparse(MBS_TEST_URL).netloc


def image_copy_put(
        project, source_mbid, target_mbid, image_id):
    return {
        'method': 'PUT',
        'url': f'http://mbid-{target_mbid}.s3.example.com/' +
               f'mbid-{target_mbid}-{image_id}.jpg',
        'headers': {
            'authorization': 'LOW user:pass',
            'x-amz-copy-source': f'/mbid-{source_mbid}/' +
                                 f'mbid-{source_mbid}-{image_id}.jpg',
            'x-archive-auto-make-bucket': '1',
            'x-archive-keep-old-version': '1',
            'x-archive-meta-collection': project['ia_collection'],
            'x-archive-meta-mediatype': 'image',
            'x-archive-meta-noindex': 'true',
        },
        'data': None,
    }


def index_event(mbid, **kwargs):
    return {
        'state': 'queued',
        'action': 'index',
        'message': {'gid': mbid},
        'depends_on': None,
        'attempts': 0,
        **kwargs
    }


def index_json_put(project, mbid, images):
    entity_type = project['entity_table']
    image_loc = f"https://{project['domain']}/{entity_type}/{mbid}"

    return {
        'method': 'PUT',
        'url': f'http://mbid-{mbid}.s3.example.com/index.json',
        'headers': {
            'authorization': 'LOW user:pass',
            'content-type': 'application/json; charset=US-ASCII',
            'x-archive-auto-make-bucket': '1',
            'x-archive-keep-old-version': '1',
            'x-archive-meta-collection': project['ia_collection'],
        },
        'data': json.dumps({
            'images': [
                {
                    **image,
                    'image': f"{image_loc}/{image['id']}.jpg",
                    'thumbnails': {
                        '1200': f"{image_loc}/{image['id']}-1200.jpg",
                        '250': f"{image_loc}/{image['id']}-250.jpg",
                        '500': f"{image_loc}/{image['id']}-500.jpg",
                        'large': f"{image_loc}/{image['id']}-500.jpg",
                        'small': f"{image_loc}/{image['id']}-250.jpg",
                    },
                } for image in images
            ],
            f'{entity_type}': f'https://musicbrainz.org/{entity_type}/{mbid}',
        }, sort_keys=True)
    }


def mb_metadata_xml_get(project, mbid):
    entity_type = project['entity_table']
    ws_inc_params = project['ws_inc_params']
    return {
        'method': 'GET',
        'url': f'{MBS_TEST_URL}/ws/2/{entity_type}/{mbid}?inc={ws_inc_params}',
        'headers': {'mb-set-database': 'TEST_ARTWORK_INDEXER'},
        'data': None,
    }


def mb_metadata_xml_put(project, mbid, xml):
    return {
        'method': 'PUT',
        'url': f'http://mbid-{mbid}.s3.example.com/' +
               f'mbid-{mbid}_mb_metadata.xml',
        'headers': {
            'authorization': 'LOW user:pass',
            'content-type': 'application/xml; charset=UTF-8',
            'x-archive-auto-make-bucket': '1',
            'x-archive-meta-collection': project['ia_collection'],
        },
        'data': xml,
    }


def record_items(rec):
    # ignore datetime columns
    for (key, value) in rec.items():
        if key not in ('created', 'last_updated'):
            if key == 'message':
                value = json.loads(value)
            yield (key, value)


class MockResponse():

    def __init__(self, status=200, text=''):
        self._status = status
        self._text = text

    def __await__(self):
        yield

    async def __aenter__(self):
        return self

    async def __exit__(self):
        return self

    async def text(self):
        return self._text


class MockClientSession():

    def __init__(self):
        self.last_requests = []
        self.next_responses = []
        self.session = ClientSession(raise_for_status=True)

    async def __aenter__(self, *args):
        return self

    async def __aexit__(self, *args):
        return self

    def _get_next_response(self):
        if self.next_responses:
            resp = self.next_responses.pop(0)
            if resp._status != 200:
                raise Exception('HTTP %d' % resp._status)
            return resp
        return MockResponse()

    def get(self, url, headers=None):
        self.last_requests.append({
            'method': 'GET',
            'url': url,
            'headers': headers,
            'data': None
        })
        netloc = urlparse(url).netloc
        if netloc == MBS_TEST_NETLOC:
            return self.session.get(url, headers=headers)
        return self._get_next_response()

    def put(self, url, headers=None, data=None):
        self.last_requests.append({
            'method': 'PUT',
            'url': url,
            'headers': headers,
            'data': data.decode('utf-8') if data else None,
        })
        return self._get_next_response()

    def delete(self, url, headers=None):
        self.last_requests.append({
            'method': 'DELETE',
            'url': url,
            'headers': headers,
            'data': None
        })
        return self._get_next_response()

    async def close(self):
        await self.session.close()


class TestArtArchive(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.last_requests = []
        self.next_responses = []

        self.pg_conn = await asyncpg.connect(**tests_config['database'])
        self.session = MockClientSession()

        @asynccontextmanager
        async def http_client_cls(*args, **kwargs):
            try:
                if self.session.session.closed:
                    self.session = MockClientSession()
                yield self.session
            finally:
                await self.session.close()

        self.http_client_cls = http_client_cls

    async def asyncTearDown(self):
        await self.pg_conn.close()
        await self.session.close()

    async def get_event_queue(self):
        events = await self.pg_conn.fetch(dedent('''
            SELECT * FROM artwork_indexer.event_queue
            WHERE state != 'completed'
            ORDER BY id
        '''))
        return [dict(record_items(x)) for x in events]
