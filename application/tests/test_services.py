import pytest
import datetime
import hashlib
import binascii
import tempfile
import base64
from pymongo import MongoClient, TEXT
from nameko.testing.services import worker_factory
import bson.json_util
import gridfs

from application.services.referential import ReferentialService


@pytest.fixture
def database(db_url):
    client = MongoClient(db_url)

    yield client['test_db']

    client.drop_database('test_db')
    client.close()


def test_add_entity(database):
    service = worker_factory(ReferentialService, database=database)

    service.add_entity('0', 'Notorious BIG', 'me', 'mc',
                       {'first_name': 'Christopher', 'last_name': 'Wallace', 'aka': 'Biggie Small'})
    result = database.entities.find_one({'id': '0'})
    assert result['common_name'] == 'Notorious BIG'

    service.add_entity('1', 'Big L', 'me', 'mc',
                       {'first_name': 'Lamont', 'last_name': 'Coleman'})
    result = database.entities.find({'$text': {'$search': 'big'}})

    assert len(list(result)) == 2


def test_add_translation(database):
    service = worker_factory(ReferentialService, database=database)

    database.entities.insert_one({'id': '0', 'common_name': 'The Hangover', 'provider': 'me',
                                  'type': 'movie', 'informations': {'starring': 'Bradley Cooper'}})
    database.entities.create_index('id')

    service.add_translation_to_entity('0', 'fr', 'La gueule de bois')
    result = database.entities.find_one({'id': '0'})
    assert result['internationalization'][0]['translation'] == 'La gueule de bois'


def test_delete_translation(database):
    service = worker_factory(ReferentialService, database=database)

    database.entities.insert_one({'id': '0', 'common_name': 'The Hangover', 'provider': 'me',
                                  'type': 'movie', 'informations': {'starring': 'Bradley Cooper'},
                                  'internationalization': [{'language': 'fr', 'translation': 'La gueule de bois'}]})
    database.entities.create_index('id')

    service.delete_translation_from_entity('0', 'fr')
    result = database.entities.find_one({'id': '0'})
    assert len(result['internationalization']) == 0


def test_get_entity_by_id(database):
    service = worker_factory(ReferentialService, database=database)

    database.entities.insert_one({'id': '0', 'common_name': 'The Hangover', 'provider': 'me',
                                  'type': 'movie', 'informations': {'starring': 'Bradley Cooper'}})
    database.entities.create_index('id')

    result = bson.json_util.loads(service.get_entity_by_id('0'))
    assert result['common_name'] == 'The Hangover'


def test_get_entity_by_name(database):
    service = worker_factory(ReferentialService, database=database)
    database.entities.insert_one({'id': '0', 'common_name': 'The Hangover', 'provider': 'me',
                                  'type': 'movie', 'informations': {'starring': 'Bradley Cooper'},
                                  'internationalization': [{'language': 'fr', 'translation': 'la gueule de bois'}]})
    database.entities.create_index('id')
    database.entities.create_index([('common_name', TEXT),
                                    ('internationalization.translation', TEXT)],
                                   default_language='english')
    result = bson.json_util.loads(service.get_entity_by_name('hangover'))
    assert result[0]['id'] == '0'

    result = bson.json_util.loads(service.get_entity_by_name('gueule'))
    assert result[0]['id'] == '0'

    result = bson.json_util.loads(service.get_entity_by_name('nothing comparable'))
    assert len(result) == 0


def test_add_timeline_entry(database):
    service = worker_factory(ReferentialService, database=database)

    database.entities.insert_one({'id': '0', 'common_name': 'The Hangover', 'provider': 'me',
                                  'type': 'movie', 'informations': {'starring': 'Bradley Cooper'}})
    database.entities.create_index('id')

    service.add_timeline_entry('0', datetime.datetime.now().isoformat(), 'me', 'new', 'reading', 'new movie')

    result = database.entities.find_one({'id': '0'})
    assert result['timeline'][0]


def test_add_picture_to_entity(database):
    service = worker_factory(ReferentialService, database=database)
    database.entities.insert_one({'id': '0', 'common_name': 'The Hangover', 'provider': 'me',
                                  'type': 'movie', 'informations': {'starring': 'Bradley Cooper'}})
    database.entities.create_index('id')

    pic = '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'
    filename = hashlib.sha1(''.join(['0', 'mycontext', 'myformat']).encode('utf-8')).hexdigest()

    service.add_picture_to_entity('0', 'mycontext', 'myformat', pic)
    fs = gridfs.GridFS(database)
    file = fs.find_one({'filename': filename})

    assert file


def test_delete_picture_from_entity(database):
    service = worker_factory(ReferentialService, database=database)
    database.entities.insert_one({'id': '0', 'common_name': 'The Hangover', 'provider': 'me',
                                  'type': 'movie', 'informations': {'starring': 'Bradley Cooper'}})
    database.entities.create_index('id')

    pic = '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'
    fs = gridfs.GridFS(database)

    filename = hashlib.sha1(''.join(['0', 'mycontext', 'myformat']).encode('utf-8')).hexdigest()
    with tempfile.TemporaryFile() as f:
        f.write(binascii.hexlify(base64.b64decode(pic)))
        f.flush()
        f.seek(0)
        fs.put(f, filename=filename)

    service.delete_picture_from_entity('0', 'mycontext', 'myformat')

    assert not fs.find_one({'filename': filename})


def test_get_entity_picture(database):
    service = worker_factory(ReferentialService, database=database)
    database.entities.insert_one({'id': '0', 'common_name': 'The Hangover', 'provider': 'me',
                                  'type': 'movie', 'informations': {'starring': 'Bradley Cooper'}})
    database.entities.create_index('id')

    pic = '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'
    fs = gridfs.GridFS(database)

    filename = hashlib.sha1(''.join(['0', 'mycontext', 'myformat']).encode('utf-8')).hexdigest()
    with tempfile.TemporaryFile() as f:
        f.write(binascii.hexlify(base64.b64decode(pic)))
        f.flush()
        f.seek(0)
        fs.put(f, filename=filename)

    entity_pic = service.get_entity_picture('0', 'mycontext', 'myformat')

    assert entity_pic == pic
