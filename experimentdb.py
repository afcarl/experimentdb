from __future__ import print_function
from simplekv.fs import FilesystemStore
from simplekv.memory.redisstore import RedisStore
from redis import StrictRedis
import json
import os
import pandas

class Key(dict):
    '''A non-mutable dictionary to access entries in the database.

    A :class:`Key` is composed of zero or more sub-keys (the keys of the dictionary).
    '''

    def matches(self, other):
        '''Return true if this key (partially) matches `other`.

        A key matches another if each subkey and value is contained in `other`. 
        An empty key matches everything.
        '''
        if len(self) == 0:
            return True
        if len(self) > len(other):
            return False
        return self.viewitems() <= other.viewitems()

    def __key(self):
        return tuple(sorted(self.items()))

    def __repr__(self):
        return "{0}({1})".format(self.__class__.__name__,
            ", ".join("{0}={1}".format(
                    str(i[0]),repr(i[1])) for i in self.__key()))

    def __hash__(self):
        return hash(self.__key())

    def __setitem__(self, key, value):
        raise TypeError("{0} does not support item assignment"
                         .format(self.__class__.__name__))

    def __delitem__(self, key):
        raise TypeError("{0} does not support item assignment"
                         .format(self.__class__.__name__))

    def clear(self):
        raise TypeError("{0} does not support item assignment"
                         .format(self.__class__.__name__))

    def pop(self, *args, **kwargs):
        raise TypeError("{0} does not support item assignment"
                         .format(self.__class__.__name__))

    def popitem(self, *args, **kwargs):
        raise TypeError("{0} does not support item assignment"
                         .format(self.__class__.__name__))

    def setdefault(self, *args, **kwargs):
        raise TypeError("{0} does not support item assignment"
                         .format(self.__class__.__name__))

    def update(self, *args, **kwargs):
        raise TypeError("{0} does not support item assignment"
                         .format(self.__class__.__name__))

    def __add__(self, right):
        result = Key(self)
        dict.update(result, right)
        return result

class Value(dict):
    pass

class DataBase(object):

    def __init__(self, store):
        self.store = store

    def put(self, key, value, allow_update=False):

        assert isinstance(key, Key)
        assert isinstance(value, Value)

        h = str(hash(key))

        if not allow_update and h in self.store:
            raise RuntimeError("Key " + repr(key) + " already in database, use 'allow_update=True' if you want to replace an entry.")
        self.store.put(h, json.dumps((key, value)))

    def get(self, key = None):

        if key is None:
            key = Key()

        rows = []

        for h in self.store:
            k, v = json.loads(self.store.get(h))
            if key.matches(k):
                k.update(v)
                rows.append(k)

        return pandas.DataFrame(rows)

    def delete(self, key):

        h = str(hash(key))
        self.store.delete(h)

    def clear(self):

        for h in self.store:
            self.store.delete(h)

    def backfill(self, subkey, value, limit_to=None):
        '''Add a new subkey with a given value to previous entries. Optionally 
        limit to entries matching `limit_to` key. Only entries that do not yet 
        have the subkey will be altered.
        '''

        if limit_to is None:
            limit_to = Key()

        for h in self.store:
            k, v = json.loads(self.store.get(h))
            k = Key(k)
            v = Value(v)
            # if subkey is in k, it is not None
            if subkey in k:
                continue
            if limit_to.matches(k):
                self.delete(k)
                k = k + {subkey: value}
                self.put(k, v)

def open(db_name, backend='filesystem', **kwargs):

    path = os.path.join(os.path.expanduser('~'), '.experimentdb', db_name)

    if backend == 'filesystem':

        print("Using filesystem backend")

        if not os.path.exists(path):
            try:
                os.makedirs(path)
            except Exception as e:
                print(e)
                pass
        return DataBase(FilesystemStore(path))

    if backend == 'redis':

        print("Using redis backend")

        # find or create a new db index
        redis_config_store = RedisStore(StrictRedis(db=0, **kwargs))

        if db_name in redis_config_store:

            db_index = int(redis_config_store.get(db_name))
            print("Database", db_name, "already exists with index", db_index)

        else:

            if 'next_db_index' in redis_config_store:
                next_db_index = int(redis_config_store.get('next_db_index'))
                db_index = next_db_index
                next_db_index += 1
                redis_config_store.put('next_db_index', str(next_db_index))
            else:
                db_index = 1
                redis_config_store.put('next_db_index', '2')

            print("New database", db_name, "created with index", db_index)
            redis_config_store.put(db_name, db_index)

        return DataBase(RedisStore(StrictRedis(db=db_index, **kwargs)))
