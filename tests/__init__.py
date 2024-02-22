import configparser
import json
import unittest
from textwrap import dedent

from pg_conn_wrapper import PgConnWrapper


tests_config = configparser.ConfigParser()
tests_config.read('config.tests.ini')


MBS_TEST_URL = tests_config['musicbrainz']['url']


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
            'content-type': 'application/json; charset=UTF-8',
            'x-archive-auto-make-bucket': '1',
            'x-archive-keep-old-version': '1',
            'x-archive-meta-collection': project['ia_collection'],
            'x-archive-meta-mediatype': 'image',
            'x-archive-meta-noindex': 'true',
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
        }, sort_keys=True).encode('utf-8')
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
            'x-archive-meta-mediatype': 'image',
            'x-archive-meta-noindex': 'true',
        },
        'data': xml.encode('utf-8'),
    }


def record_items(rec):
    # ignore datetime columns
    for (key, value) in rec.items():
        if key not in ('created', 'last_updated'):
            yield (key, value)


class MockResponse():

    def __init__(self, status=200, content=''):
        self.status = status
        if isinstance(content, str):
            content = content.encode('utf-8')
        self.content = content

    def raise_for_status(self):
        if self.status < 200 or self.status >= 300:
            raise Exception('Error: HTTP ' + str(self.status))


class MockClientSession():

    def __init__(self):
        self.last_requests = []
        self.next_responses = []
        self.headers = {}

    def _get_next_response(self):
        resp = self.next_responses.pop(0)
        if resp.status < 200 or resp.status >= 300:
            raise Exception('HTTP %d' % resp.status)
        return resp

    def get(self, url, **kwargs):
        self.last_requests.append({
            'method': 'GET',
            'url': url,
            'headers': kwargs.get('headers'),
            'data': None
        })
        return self._get_next_response()

    def put(self, url, **kwargs):
        self.last_requests.append({
            'method': 'PUT',
            'url': url,
            'headers': kwargs.get('headers'),
            'data': kwargs.get('data'),
        })
        return self._get_next_response()

    def delete(self, url, **kwargs):
        self.last_requests.append({
            'method': 'DELETE',
            'url': url,
            'headers': kwargs.get('headers'),
            'data': None
        })
        return self._get_next_response()

    def close(self):
        pass


class TestArtArchive(unittest.TestCase):

    def setUp(self):
        self.pg_conn = PgConnWrapper(tests_config)
        self.session = MockClientSession()
        self.http_client_cls = lambda: self.session

    def tearDown(self):
        self.pg_conn.close()
        self.session.close()

    def get_event_queue(self):
        pg_cur = self.pg_conn.execute(dedent('''
            SELECT * FROM artwork_indexer.event_queue
            WHERE state != 'completed'
            ORDER BY id
        '''))
        return [dict(record_items(x)) for x in pg_cur.fetchall()]
