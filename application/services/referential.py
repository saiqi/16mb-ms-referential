import hashlib
import binascii
import tempfile
import base64
import datetime
import itertools
import string
from nameko.rpc import rpc
from nameko.events import event_handler
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

    def _add_provider_subscription(self, user, provider):
        for collection in ('entities', 'events', 'search'):
            self.database[collection].update_many({'provider': provider},
                {'$addToSet':{'allowed_users': user}})

    def _delete_provider_subscription(self, user, providers):
        for collection in ('entities', 'events', 'search'):
            self.database[collection].update_many({'provider': {'$in': providers}}, 
                {'$pull': {'allowed_users': user}})

    @event_handler('subscription_manager', 'user_sub')
    def handle_suscription(self, payload):
        user = payload['user']
        if 'referential' in payload['subscription']:
            referential = payload['subscription']['referential']
            old_sub = self.database.subscriptions.find_one({'user': user})
            if 'providers' in referential:
                if old_sub:
                    old_providers = set([r for r in old_sub['subscription']['providers']])
                    new_providers = set(referential['providers'])
                    diff = old_providers - new_providers
                    self._delete_provider_subscription(user, list(diff))
                for provider in referential['providers']:
                    self._add_provider_subscription(user, provider)
            self.database.subscriptions.update_one({'user': user},
                {'$set': {'subscription': referential}}, upsert=True)

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

    def _get_allowed_users(self, provider):
        sub = self.database.subscriptions.find({'subscription.providers': provider}, {'user': 1})
        return [r['user'] for r in sub]

    @rpc
    def add_entity(self, id, common_name, provider, type, informations):
        self.database.entities.create_index([('id', ASCENDING), ('allowed_users', ASCENDING)])
        self.database.entities.create_index([('common_name', TEXT)], default_language='english')
        self.database.entities.update_one(
            {'id': id},
            {'$set':
                {
                    'common_name': common_name,
                    'provider': provider,
                    'informations': informations,
                    'type': type,
                    'allowed_users': self._get_allowed_users(provider)
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
            raise ReferentialServiceError('Entity not found with id {}'.format(id))

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
    def add_multiline_to_entity(self, id, multiline):
        entity = self.database.entities.find_one({'id': id}, {'id': 1, 'multiline': 1})

        if not entity:
            raise ReferentialServiceError('Entity not found with id {}'.format(id))

        self.database.entities.update_one({'id': id}, {'$set': {'multiline': multiline}})

        return {'id': id}

    @rpc 
    def delete_multiline_from_entity(self, id):
        self.database.entities.update_one({'id': id}, {'$unset': {'multiline': ''}})

        return {'id': id}

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
    def get_entity_by_id(self, id, user):
        entity = self.database.entities.find_one({'id': id, 'allowed_users': user}, 
            {'_id': 0, 'allowed_users': 0})
        return bson.json_util.dumps(entity)

    @rpc
    def get_entities_by_name(self, name, user):
        cursor = self.database.entities.find({'$text': {'$search': name}, 'allowed_users':user}, 
            {'_id': 0, 'allowed_users': 0})
        return bson.json_util.dumps(list(cursor))

    def _check_gridfs_access(self, id, context, user):
        sub = self.database.subscriptions.find_one({
            'user': user, 
            'subscription.pictures': context
        })
        if not sub:
            return False
        entity = self.database.entities.find_one({'id': id, 'allowed_users': user})
        if not entity:
            return False
        return True

    @rpc
    def get_entity_picture(self, id, context, format, user):
        if not self._check_gridfs_access(id, context, user):
            return None
        fs = gridfs.GridFS(self.database)
        filename = self._filename('bitmap', id, context, format)

        file = fs.find_one({'filename': filename})

        if file:
            return base64.b64encode(binascii.unhexlify(file.read())).decode('utf-8')

        return None

    @rpc
    def get_entity_logo(self, id, context, format, user):
        if not self._check_gridfs_access(id, context, user):
            return None
        fs = gridfs.GridFS(self.database)
        filename = self._filename('vectorial', id, context, format)

        file = fs.find_one({'filename': filename})

        if file:
            return file.read().decode('utf-8')

        return None

    @rpc
    def add_event(self, id, date, provider, type, common_name, content, entities):
        self.database.events.create_index([('id', ASCENDING), ('allowed_users', ASCENDING)])
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
                    'entities': entities,
                    'allowed_users': self._get_allowed_users(provider)
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
            ('ngrams', TEXT), ('prefix_ngrams', TEXT)
        ], weights={'ngrams': 100, 'prefix_ngrams': 200})

        entities = self.database.entities.find({},{
            'id': 1,
            'common_name': 1,
            'type': 1,
            'provider': 1,
            'allowed_users': 1,
            '_id': 0})
        events = self.database.events.find({},{
            'id': 1,
            'common_name': 1,
            'type': 1,
            'provider': 1,
            'allowed_users': 1,
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
                    'provider': ref_entry['provider'],
                    'allowed_users': ref_entry['allowed_users']
                }
            }, upsert=True)
        return True

    @rpc
    def update_entry_ngrams(self, entry_id):
        self.database.search.create_index([
            ('ngrams', TEXT), ('prefix_ngrams', TEXT)
        ], weights={'ngrams': 100, 'prefix_ngrams': 200})
        project = {'id': 1,'common_name': 1,'type': 1,'provider': 1, 'allowed_users': 1,'_id': 0}
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
                    'provider': entry['provider'],
                    'allowed_users': entry['allowed_users']
                }
        }, upsert=True)
        return entry_id

    @rpc
    def get_event_by_id(self, id, user):
        event = self.database.events.find_one({'id': id, 'allowed_users': user}, 
            {'_id': 0, 'allowed_users': 0})
        return bson.json_util.dumps(event)

    @rpc
    def get_events_by_entity_id(self, entity_id, user):
        cursor = self.database.events.find({'entities.id': entity_id, 'allowed_users': user}, 
            {'_id': 0, 'allowed_users': 0})
        return bson.json_util.dumps(list(cursor))

    @rpc
    def get_events_by_name(self, name, user):
        cursor = self.database.events.find({'$text': {'$search': name}, 'allowed_users': user}, 
            {'_id': 0, 'allowed_users': 0})
        return bson.json_util.dumps(list(cursor))

    @rpc
    def get_events_between_dates(self, start_date, end_date, user):
        cursor = self.database.events.find({'date': {'$gte': dateutil.parser.parse(start_date),'$lt': dateutil.parser.parse(end_date)},
            'allowed_users': user}, {'_id': 0})
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
    def search_entity(self, name, user, type=None, provider=None):
        query = {'$text': {'$search': name}, 'allowed_users': user}
        if type is not None:
            query['type'] = type
        if provider is not None:
            query['provider'] = provider
        cursor = self.database.entities.find(query, {'_id': 0, 'allowed_users': 0})
        return bson.json_util.dumps(list(cursor))

    @rpc
    def search_event(self, name, date, user, type=None, provider=None):
        start_date = dateutil.parser.parse(date)
        end_date = start_date + datetime.timedelta(days=1)

        query = {
            'date': {
                '$gte': start_date,
                '$lt': end_date
            },
            '$text': {
                '$search': name
            },
            'allowed_users': user
        }

        if type is not None:
            query['type'] = type
        if provider is not None:
            query['provider'] = provider

        cursor = self.database.events.find(query, {'_id': 0, 'allowed_users': 0})
        return bson.json_util.dumps(list(cursor))

    @rpc
    def fuzzy_search(self, query, user, type=None, provider=None):
        query = {
            '$text': {'$search': self._make_ngrams(query)},
            'allowed_users': user
        }

        if type is not None:
            query['type'] = type
        if provider is not None:
            query['provider'] = provider

        cursor = self.database.search.find(
            query,
            {'id': 1, 'common_name': 1, 'score': {'$meta': 'textScore'}, '_id': 0}
            ).sort([('score', {'$meta': 'textScore'})])
        return bson.json_util.dumps(list(cursor))
