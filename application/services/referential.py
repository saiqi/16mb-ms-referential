import hashlib
import binascii
import tempfile
import base64
from nameko.rpc import rpc
from nameko_mongodb.database import MongoDatabase
from pymongo import TEXT
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
    def add_timeline_entry(self, id, date, provider, type, source, content):
        p_date = dateutil.parser.parse(date)
        self.database.entities.update_one(
            {'id': id},
            {
                '$addToSet': {
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

        return {'id': id, 'date': date, 'provider': provider, 'type': type, 'source': source}

    @rpc
    def get_entity_by_id(self, id, with_timeline=False):
        entity = self.database.entities.find_one({'id': id}, {'_id': 0, 'timeline': with_timeline})
        return bson.json_util.dumps(entity)

    @rpc
    def get_entity_by_name(self, name, with_timeline=False):
        cursor = self.database.entities.find({'$text': {'$search': name}}, {'_id': 0, 'timeline': with_timeline})
        return bson.json_util.dumps(list(cursor))

    @rpc
    def get_entity_picture(self, id, context, format):
        fs = gridfs.GridFS(self.database)
        filename = self._filename(id, context, format)

        file = fs.find_one({'filename': filename})

        if file:
            return base64.b64encode(binascii.unhexlify(file.read())).decode('utf-8')

        return None
