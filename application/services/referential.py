import hashlib
import binascii
import tempfile
import base64
from nameko.rpc import rpc
from nameko_mongodb.database import MongoDatabase
from pymongo import TEXT, ASCENDING, DESCENDING
import gridfs
import bson.json_util
import dateutil.parser


class ReferentialService(object):
    name = 'referential'

    database = MongoDatabase(result_backend=False)

    @staticmethod
    def _filename(entity_id, context_id, format_id):
        concat = ''.join([entity_id, context_id, format_id])
        return hashlib.sha1(concat.encode('utf-8')).hexdigest()

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

        return {'id': id}

    @rpc
    def add_translation_to_entity(self, id, language, translation):
        self.database.entities.update_one(
            {'id': id},
            {'$addToSet': {'internationalization': {'language': language, 'translation': translation}}})

        return {'id': id, 'language': language}

    @rpc
    def delete_translation_from_entity(self, id, language):
        self.database.entities.update_one({'id': id}, {'$pull': {'internationalization': {'language': language}}})

        return {'id': id, 'language': language}

    @rpc
    def add_picture_to_entity(self, id, context, format, picture_b64):
        fs = gridfs.GridFS(self.database)
        filename = self._filename(id, context, format)

        file = fs.find_one({'filename': filename})

        if file:
            fs.delete(file._id)

        with tempfile.TemporaryFile() as f:
            f.write(binascii.hexlify(base64.b64decode(picture_b64)))
            f.flush()
            f.seek(0)
            fs.put(f, filename=filename)

        return {'id': id, 'context': context, 'format': format}

    @rpc
    def delete_picture_from_entity(self, id, context, format):
        fs = gridfs.GridFS(self.database)
        filename = self._filename(id, context, format)

        file = fs.find_one({'filename': filename})

        if file:
            fs.delete(file._id)

        return {'id': id, 'context': context, 'format': format}

    @rpc
    def get_entity_by_id(self, id):
        entity = self.database.entities.find_one({'id': id}, {'_id': 0})
        return bson.json_util.dumps(entity)

    @rpc
    def get_entities_by_name(self, name):
        cursor = self.database.entities.find({'$text': {'$search': name}}, {'_id': 0})
        return bson.json_util.dumps(list(cursor))

    @rpc
    def get_entity_picture(self, id, context, format):
        fs = gridfs.GridFS(self.database)
        filename = self._filename(id, context, format)

        file = fs.find_one({'filename': filename})

        if file:
            return base64.b64encode(binascii.unhexlify(file.read())).decode('utf-8')

        return None

    @rpc
    def add_event(self, id, date, provider, type, common_name, content, entities):
        self.database.events.create_index([('entities.id', ASCENDING), ('date', DESCENDING)])
        self.database.events.create_index('id', unique=True)
        self.database.events.create_index([('common_name', TEXT)], default_language='english')

        p_date = dateutil.parser.parse(date)

        self.database.events.update_one(
            {'id': id},
            {
                '$set': {
                    'date': p_date,
                    'provider': provider,
                    'type': type,
                    'common_name': common_name,
                    'content': content,
                    'entities': entities
                }
            }, upsert=True
        )

        return {'id': id, 'date': date, 'provider': provider, 'type': type, 'common_name': common_name}

    @rpc
    def get_event_by_id(self, id):
        event = self.database.events.find_one({'id': id}, {'_id': 0})
        return bson.json_util.dumps(event)

    @rpc
    def get_events_by_entity_id(self, entity_id):
        cursor = self.database.events.find({'entities.id': entity_id}, {'_id': 0})
        return bson.json_util.dumps(list(cursor))

    @rpc
    def get_events_by_name(self, name):
        cursor = self.database.events.find({'$text': {'$search': name}}, {'_id': 0})
        return bson.json_util.dumps(list(cursor))

    @rpc
    def add_label(self, id, language, label):
        self.database.labels.create_index([('id', ASCENDING), ('language', ASCENDING)], unique=True)

        self.database.labels.update_one({'id': id, 'language': language},
                                        {'$set': {'label': label}}, upsert=True)

        return {'id': id, 'language': language}

    @rpc
    def delete_label(self, id, language):
        self.database.labels.delete_one({'id': id, 'language': language})

        return {'id': id, 'language': language}

    @rpc
    def get_labels_by_id_and_language(self, ids, language):
        if type(ids) == list:
            cursor = self.database.labels.find({
                'id': {'$in': ids},
                'language': language}, {'_id': 0})
            return list(cursor)

        return self.database.labels.find_one({'id': ids, 'language': language}, {'_id': 0})

    @rpc
    def get_labels_by_id(self, ids):
        if type(ids) == list:
            cursor = self.database.labels.find({'id': {'$in': ids}}, {'_id': 0})
            return list(cursor)

        return list(self.database.labels.find({'id': ids}, {'_id': 0}))
