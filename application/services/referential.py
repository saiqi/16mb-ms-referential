from nameko.rpc import rpc
from nameko_mongodb.database import MongoDatabase
from pymongo import TEXT
import bson.json_util
import dateutil.parser

class ReferentialService(object):
    name = 'referential'

    database = MongoDatabase(result_backend=False)

    @rpc
    def add_entity(self, id, common_name, provider, type, informations):
        self.database.entities.create_index('id')

        self.database.entities.create_index([('common_name', TEXT),
                                            ('internationalization.translation', TEXT)],
                                            default_language='english')

        self.database.entities.update_one(
            {'id': id},
            {'$set':
                {
                    'common_name': common_name,
                    'provider': provider,
                    'informations': informations,
                    'type': type
                }
            }, upsert=True)

    @rpc
    def add_translation(self, id, language, translation):
        self.database.entities.update_one(
            {'id': id},
            {'$push': {'internationalization': {'language': language, 'translation': translation}}})

    @rpc
    def add_timeline_entry(self, id, date, provider, type, source, content):
        p_date = dateutil.parser.parse(date)
        self.database.entities.update_one(
            {'id': id},
            {'$push': {
                     'timeline': {
                         'date': p_date,
                         'provider': provider,
                         'type': type,
                         'source': source,
                         'content': content
                     }
                 }
            }
        )

    @rpc
    def get_entity_by_id(self, id):
        entity = self.database.entities.find_one({'id': id}, {'_id': 0})
        return bson.json_util.dumps(entity)

    @rpc
    def get_entity_by_name(self, name):
        cursor = self.database.entities.find({'$text': {'$search': name}}, {'_id': 0})
        return bson.json_util.dumps(list(cursor))
