import hashlib
import binascii
import tempfile
import base64
import datetime
import itertools
import string
from nameko.rpc import rpc
from nameko_mongodb.database import MongoDatabase
from pymongo import TEXT, ASCENDING, DESCENDING
import gridfs
import bson.json_util
import dateutil.parser


class ReferentialServiceError(Exception):
    pass


class ReferentialService(object):
    name = 'referential'

    database = MongoDatabase(result_backend=False)

    @staticmethod
    def _filename(_type, entity_id, context_id, format_id):
        concat = ''.join([_type, entity_id, context_id, format_id])
        return hashlib.sha1(concat.encode('utf-8')).hexdigest()

    def _add_file_to_gridfs(self, filename, content, is_base64=False):
        fs = gridfs.GridFS(self.database)

        file = fs.find_one({'filename': filename})

        if file:
            fs.delete(file._id)

        with tempfile.TemporaryFile() as f:
            if is_base64 is True:
                f.write(binascii.hexlify(base64.b64decode(content)))
            else:
                f.write(content.encode('utf-8'))
            f.flush()
            f.seek(0)
            fs.put(f, filename=filename)

    def _delete_file_from_gridfs(self, filename):
        fs = gridfs.GridFS(self.database)

        file = fs.find_one({'filename': filename})

        if file:
            fs.delete(file._id)

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
    def add_informations_to_entity(self, id, informations):
        update_doc = dict(('informations.{}'.format(k), v)
                          for k, v in informations.items())

        self.database.entities.update_one(
            {'id': id},
            {'$set': update_doc}
        )

        return {'id': id}

    @rpc
    def add_translation_to_entity(self, id, language, translation):
        entity = self.database.entities.find_one({'id': id}, {'id': 1, 'internationalization': 1})

        if not entity:
            raise ReferentialServiceError('No entity found with id {}'.format(id))

        if 'internationalization' not in entity:
            entity['internationalization'] = {language: translation}
        else:
            entity['internationalization'][language] = translation

        self.database.entities.update_one(
            {'id': id},
            {'$set': {'internationalization': entity['internationalization']}})

        return {'id': id, 'language': language}

    @rpc
    def delete_translation_from_entity(self, id, language):
        self.database.entities.update_one({'id': id}, {'$unset': {'internationalization': {language: ''}}})

        return {'id': id, 'language': language}

    @rpc
    def add_picture_to_entity(self, id, context, format, picture_b64):
        filename = self._filename('bitmap', id, context, format)
        self._add_file_to_gridfs(filename, picture_b64, is_base64=True)
        return {'id': id, 'context': context, 'format': format}

    @rpc
    def add_logo_to_entity(self, id, context, format, svg_string):
        filename = self._filename('vectorial', id, context, format)
        self._add_file_to_gridfs(filename, svg_string)
        return {'id': id, 'context': context, 'format': format}

    @rpc
    def delete_picture_from_entity(self, id, context, format):
        filename = self._filename('bitmap', id, context, format)
        self._delete_file_from_gridfs(filename)
        return {'id': id, 'context': context, 'format': format}

    @rpc
    def delete_logo_from_entity(self, id, context, format):
        filename = self._filename('vectorial', id, context, format)
        self._delete_file_from_gridfs(filename)
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
        filename = self._filename('bitmap', id, context, format)

        file = fs.find_one({'filename': filename})

        if file:
            return base64.b64encode(binascii.unhexlify(file.read())).decode('utf-8')

        return None

    @rpc
    def get_entity_logo(self, id, context, format):
        fs = gridfs.GridFS(self.database)
        filename = self._filename('vectorial', id, context, format)

        file = fs.find_one({'filename': filename})

        if file:
            return file.read().decode('utf-8')

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

    @staticmethod
    def _make_ngrams(words, min_size=3, prefix_only=False):
        ngrams = []
        table = str.maketrans({key: None for key in string.punctuation})
        for word in words.lower().split(' '):
            clean_word = word.translate(table)
            length = len(clean_word)
            size_range = range(min_size, max(min_size, length) + 1)
            if prefix_only:
                ngrams.extend(clean_word[0: size] for size in size_range)
            ngrams.extend(clean_word[i: i+size] for size in size_range for i in range(0, max(0, length - size) + 1))
        return ' '.join(ngrams)

    @rpc
    def update_ngrams_search_collection(self):
        self.database.search.create_index([
            ('ngrams', TEXT), ('prefix_ngrams', TEXT), ('type', ASCENDING), ('provider', ASCENDING)
        ], weights={'ngrams': 100, 'prefix_ngrams': 200})

        entities = self.database.entities.find({},{
            'id': 1,
            'common_name': 1,
            'type': 1,
            'provider': 1,
            '_id': 0})
        events = self.database.events.find({},{
            'id': 1,
            'common_name': 1,
            'type': 1,
            'provider': 1,
            '_id': 0})
        for ref_entry in itertools.chain(entities, events):
            ngrams = self._make_ngrams(ref_entry['common_name'])
            pref_ngrams = self._make_ngrams(ref_entry['common_name'], prefix_only=True)

            self.database.search.update_one({'id': ref_entry['id']}, {
                '$set':{
                    'id': ref_entry['id'],
                    'common_name': ref_entry['common_name'],
                    'ngrams': ngrams,
                    'prefix_ngrams': pref_ngrams,
                    'type': ref_entry['type'],
                    'provider': ref_entry['provider']
                }
            }, upsert=True)
        return True

    @rpc
    def update_entry_ngrams(self, entry_id):
        project = {'id': 1,'common_name': 1,'type': 1,'provider': 1,'_id': 0}
        entry = self.database.entities.find_one({'id': entry_id}, project)
        if not entry:
            entry = self.database.events.find_one({'id': entry_id}, project)
            if not entry:
                raise ReferentialServiceError('No entry with {} found in referential'.format(entry_id))
        ngrams = self._make_ngrams(entry['common_name'])
        pref_ngrams = self._make_ngrams(entry['common_name'], prefix_only=True)

        self.database.search.update_one({'id': entry['id']}, {
            '$set':{
                    'id': entry['id'],
                    'common_name': entry['common_name'],
                    'ngrams': ngrams,
                    'prefix_ngrams': pref_ngrams,
                    'type': entry['type'],
                    'provider': entry['provider']
                }
        }, upsert=True)
        return entry_id

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
    def add_label(self, id, language, context, label):
        self.database.labels.create_index([('id', ASCENDING),
                                           ('language', ASCENDING), ('context', ASCENDING)], unique=True)

        self.database.labels.update_one({'id': id, 'language': language, 'context': context},
                                        {'$set': {'label': label}}, upsert=True)

        return {'id': id, 'language': language, 'context': context}

    @rpc
    def delete_label(self, id, language, context):
        self.database.labels.delete_one({'id': id, 'language': language, 'context': context})

        return {'id': id, 'language': language, 'context': context}

    @rpc
    def get_labels_by_id_and_language_and_context(self, ids, language, context):
        if type(ids) == list:
            cursor = self.database.labels.find({
                'id': {'$in': ids},
                'language': language, 'context': context}, {'_id': 0})
            return list(cursor)

        return self.database.labels.find_one({'id': ids, 'language': language, 'context': context}, {'_id': 0})

    @rpc
    def get_labels_by_id(self, ids):
        if type(ids) == list:
            cursor = self.database.labels.find({'id': {'$in': ids}}, {'_id': 0})
            return list(cursor)

        return list(self.database.labels.find({'id': ids}, {'_id': 0}))

    @rpc
    def search_entity(self, name, type=None, provider=None):
        query = {'$text': {'$search': name}}
        if type is not None:
            query['type'] = type
        if provider is not None:
            query['provider'] = provider
        cursor = self.database.entities.find(query, {'_id': 0})
        return bson.json_util.dumps(list(cursor))

    @rpc
    def search_event(self, name, date, type=None, provider=None):
        start_date = dateutil.parser.parse(date)
        end_date = start_date + datetime.timedelta(days=1)

        query = {
            'date': {
                '$gte': start_date,
                '$lt': end_date
            },
            '$text': {
                '$search': name
            }
        }

        if type is not None:
            query['type'] = type
        if provider is not None:
            query['provider'] = provider

        cursor = self.database.events.find(query, {'_id': 0})
        return bson.json_util.dumps(list(cursor))

    @rpc
    def fuzzy_search(self, query, type, provider):
        cursor = self.database.search.find(
            {
                '$text': {'$search': self._make_ngrams(query)},
                'type': type,
                'provider': provider
            },
            {'id': 1, 'common_name': 1, 'score': {'$meta': 'textScore'}, '_id': 0}
            ).sort([('score', {'$meta': 'textScore'})])
        return bson.json_util.dumps(list(cursor))
