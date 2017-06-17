import pytest
import datetime

from pymongo import MongoClient, TEXT
from nameko.testing.services import worker_factory
import bson.json_util

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

    service.add_translation('0', 'fr', 'La gueule de bois')
    result = database.entities.find_one({'id': '0'})
    assert result['internationalization'][0]['translation'] == 'La gueule de bois'


def test_remove_translation(database):
    service = worker_factory(ReferentialService, database=database)

    database.entities.insert_one({'id': '0', 'common_name': 'The Hangover', 'provider': 'me',
                                  'type': 'movie', 'informations': {'starring': 'Bradley Cooper'},
                                  'internationalization': [{'language': 'fr', 'translation': 'La gueule de bois'}]})
    database.entities.create_index('id')

    service.remove_translation('0', 'fr')
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
