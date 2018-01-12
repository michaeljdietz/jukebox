import sys
import os
import threading
import time
from .database import Database
from pathlib import Path
from .mp3 import MP3Object
from .mp4 import MP4Object
from abc import ABC, abstractmethod
import sqlite3
import re


class Library:
    SQL_BUFFER_SIZE=100000
    MAX_THREAD_COUNT=100
    THREAD_WAIT=0.25

    def __init__(self, library_path):
        self.library_path = library_path
        self.database = Database()
        # TODO: Build library if the database was just created
        # self.build_library()

    def build_library(self):
        song_count = 0

        pathlist = Path(self.library_path).glob('**/*.*')

        for path in pathlist:
            song_count += 1
            print("\rFound %s songs" % song_count, end='', file=sys.stderr)
            sys.stderr.flush()

            while threading.active_count() >= self.MAX_THREAD_COUNT:
                time.sleep(self.THREAD_WAIT)

            threading.Thread(target=self.prepare_song_worker, args=(str(path),)).start()

            if self.database.song_queue.qsize() >= self.SQL_BUFFER_SIZE:
                self.database.flush_song_queue()

        self.database.flush_song_queue()

        print("", file=sys.stderr)

        return self

    def prepare_song_worker(self, path):
        if path.endswith(".mp3") or path.endswith(".aac"):
            song = MP3Object(path)
        elif path.endswith(".m4a") or path.endswith(".m4b") or path.endswith(".m4p") or path.endswith(".mp4"):
            song = MP4Object(path)
        else:
            return None

        song.path = str(path)
        song.last_modified = os.path.getmtime(str(path))

        self.database.song_queue.put(song)

        return self


class DataAdapter(ABC):
    def __init__(self):
        self.connection = None
        self.connect()

    def __del__(self):
        self.close()

    @abstractmethod
    def connect(self):
        raise NotImplementedError

    @abstractmethod
    def close(self):
        raise NotImplementedError

    @abstractmethod
    def fetch_one(self, query):
        raise NotImplementedError

    @abstractmethod
    def fetch_row(self, query):
        pass

    @abstractmethod
    def fetch_all(self, query):
        raise NotImplementedError

    @abstractmethod
    def execute(self, query):
        raise NotImplementedError

    @abstractmethod
    def execute_many(self, query):
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def select():
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def quote_into(condition, value, value_type):
        raise NotImplementedError


class DbAdapter(DataAdapter):
    INT_TYPE = 0
    BIGINT_TYPE = 1
    FLOAT_TYPE = 2

    NUMERIC_DATA_TYPES = {
        INT_TYPE: INT_TYPE,
        BIGINT_TYPE: BIGINT_TYPE,
        FLOAT_TYPE: FLOAT_TYPE,
    }

    BIGINT_PATTERN = re.compile(r'^([+-]?(?:0[Xx][\da-fA-F]+|\d+(?:[eE][+-]?\d+)?))')

    def __init__(self, path):
        self.path = path
        super().__init__()

    def connect(self):
        self.connection = sqlite3.connect(self.path)

    def close(self):
        pass

    def fetch_one(self, query):
        pass

    def fetch_row(self, query):
        pass

    def fetch_all(self, query):
        pass

    def execute(self, query):
        pass

    def execute_many(self, query):
        pass

    def validate_object(self, data_object):
        pass

    def query(self, sql, bind=[]):
        pass

    @staticmethod
    def select():
        return DbSelect()

    @staticmethod
    def quote(values, value_type=None):
        if isinstance(values, list):
            return ", ".join([DbAdapter.quote(value) for value in values])

        if isinstance(values, DbSelect):
            return "(" + values.assemble() + ")"

        value = values

        if value_type is not None:
            value_type = value_type.upper()
            if value_type == DbAdapter.INT_TYPE:
                return int(value)
            elif value_type == DbAdapter.BIGINT_TYPE:
                matches = DbAdapter.BIGINT_PATTERN.search(value)
                if matches is not None:
                    return matches.group(1)
            elif value_type == DbAdapter.FLOAT_TYPE:
                return "{:10.4f}".format(values)

            return 0

        return DbAdapter.__quote(values)

    @staticmethod
    def __quote(value):
        if isinstance(value, int):
            return value
        elif isinstance(value, float):
            return "{:10.4f}".format(value)

        return "'" + re.escape(value) + "'"

    @staticmethod
    def quote_into(text, value, value_type=None, count=None):
        if count is None:
            return text.replace('?', DbAdapter.quote(value, value_type))

        while count > 0:
            if text.find('?') >= 0:
                text = text.replace('?', DbAdapter.quote(value, value_type), 1)

            count -= 1

        return text


class DbSelect:
    DISTINCT = 'distinct'
    COLUMNS = 'columns'
    FROM = 'from'
    UNION = 'union'
    WHERE = 'where'
    GROUP = 'group'
    HAVING = 'having'
    ORDER = 'order'
    LIMIT_COUNT = 'limit_count'
    LIMIT_OFFSET = 'limit_offset'
    FOR_UPDATE = 'for_update'

    INNER_JOIN = 'inner join'
    LEFT_JOIN = 'left join'
    RIGHT_JOIN = 'right join'
    FULL_JOIN = 'full join'
    CROSS_JOIN = 'cross join'
    NATURAL_JOIN = 'natural join'

    SQL_WILDCARD = '*'
    SQL_SELECT = 'SELECT'
    SQL_UNION = 'UNION'
    SQL_UNION_ALL = 'UNION ALL'
    SQL_FROM = 'FROM'
    SQL_WHERE = 'WHERE'
    SQL_DISTINCT = 'DISTINCT'
    SQL_GROUP_BY = 'GROUP BY'
    SQL_ORDER_BY = 'ORDER BY'
    SQL_HAVING = 'HAVING'
    SQL_FOR_UPDATE = 'FOR UPDATE'
    SQL_AND = 'AND'
    SQL_AS = 'AS'
    SQL_OR = 'OR'
    SQL_ON = 'ON'
    SQL_ASC = 'ASC'
    SQL_DESC = 'DESC'

    JOIN_TYPES = [INNER_JOIN, LEFT_JOIN, RIGHT_JOIN, FULL_JOIN, CROSS_JOIN, NATURAL_JOIN]
    UNION_TYPES = [SQL_UNION, SQL_UNION_ALL]

    ORDER_PATTERN = re.compile(r'(.*\W)(' + SQL_ASC + '|' + SQL_DESC + ')\b', re.IGNORECASE + re.DOTALL)

    PARTS_INIT = {
        DISTINCT: False,
        COLUMNS: {},
        FROM: [],
        UNION: [],
        WHERE: [],
        GROUP: [],
        HAVING: [],
        ORDER: [],
        LIMIT_COUNT: None,
        LIMIT_OFFSET: None,
        FOR_UPDATE: False,
    }

    def __init__(self, adapter):
        self.__adapter = adapter
        self.__parts = self.PARTS_INIT
        self.__bind = []

    def reset(self, part=None):
        if part is None:
            self.__parts = self.PARTS_INIT
            return self

        if part in self.__parts:
            self.__parts[part] = self.PARTS_INIT[part]

        return self

    def get_bind(self):
        return self.__bind

    def bind(self, bind):
        self.__bind = bind
        return self

    def distinct(self, flag=True):
        self.__parts[self.DISTINCT] = flag
        return self

    def columns(self, columns, table_name=None):
        if table_name is None:
            table_name = self.FROM

        return self.__table_cols(table_name, columns)

    def select_from(self, table_name, columns=SQL_WILDCARD, schema=None):
        return self.__join(self.FROM, table_name, None, columns, schema)

    def union(self, data_queries, union_type=SQL_UNION):
        if isinstance(data_queries, list) or isinstance(data_queries, tuple):
            for data_query in data_queries:
                item = [data_query, union_type]
                self.__parts[self.UNION].append(item)

    def join(self, table_name, condition, columns=SQL_WILDCARD, schema=None):
        return self.join_inner(table_name, condition, columns, schema)

    def join_inner(self, table_name, condition, columns=SQL_WILDCARD, schema=None):
        return self.__join(self.INNER_JOIN, table_name, condition, columns, schema)

    def join_left(self, table_name, condition, columns=SQL_WILDCARD, schema=None):
        return self.__join(self.LEFT_JOIN, table_name, condition, columns, schema)

    def join_right(self, table_name, condition, columns=SQL_WILDCARD, schema=None):
        return self.__join(self.RIGHT_JOIN, table_name, condition, columns, schema)

    def join_full(self, table_name, condition, columns=SQL_WILDCARD, schema=None):
        return self.__join(self.FULL_JOIN, table_name, condition, columns, schema)

    def join_cross(self, table_name, condition, columns=SQL_WILDCARD, schema=None):
        return self.__join(self.CROSS_JOIN, table_name, condition, columns, schema)

    def join_natural(self, table_name, condition, columns=SQL_WILDCARD, schema=None):
        return self.__join(self.NATURAL_JOIN, table_name, condition, columns, schema)

    def where(self, condition, value=None, value_type=None):
        self.__parts[self.WHERE].append(self.__where(condition, value, value_type, True))
        return self

    def or_where(self, condition, value=None, value_type=None):
        self.__parts[self.WHERE].append(self.__where(condition, value, value_type, False))
        return self

    and_where = where

    def group(self, columns):
        if isinstance(columns, str):
            columns = [columns]

        self.__parts[self.GROUP].extend(columns)

        return self

    def having(self, condition, value=None, value_type=None, boolean_type=SQL_AND):
        if value is not None:
            condition = self.__adapter.quote_into(condition, value, value_type)

        if len(self.__parts[self.HAVING]) > 1:
            self.__parts[self.HAVING].append('%s (%s)' % (self.SQL_AND, condition))
        else:
            self.__parts[self.HAVING].append(condition)

        return self

    def or_having(self, condition, value=None, value_type=None):
        return self.having(condition, value, value_type, self.SQL_OR)

    def order(self, columns):
        if isinstance(columns, str):
            columns = [columns]

        for column in columns:
            if column is None:
                continue

            direction = self.SQL_ASC
            matches = self.ORDER_PATTERN.search(column)
            if matches is not None:
                column = matches.group(1).strip()
                direction = matches.group(2)

            self.__parts[self.ORDER].append([column, direction])

        return self

    def limit(self, count=None, offset=None):
        self.__parts[self.LIMIT_COUNT] = int(count)
        self.__parts[self.LIMIT_OFFSET] = int(offset)
        return self

    def limit_page(self, page, row_count):
        page = page if page > 0 else 1
        row_count = row_count if row_count > 0 else 1
        self.__parts[self.LIMIT_COUNT] = int(row_count)
        self.__parts[self.LIMIT_OFFSET] = int(row_count * (page - 1))
        return self

    def for_update(self, flag=True):
        self.__parts[self.FOR_UPDATE]=flag
        return self

    def get_part(self, part):
        part = part.lower()
        if part in self.__parts:
            return self.__parts[part]

        raise ValueError

    def query(self, fetch_mode=None, bind=[]):
        if len(bind) > 0:
            self.bind(bind)

        statement = self.__adapter.query(self)
        if fetch_mode is None:
            fetch_mode = self.__adapter.fetch_mode

        statement.fetch_mode = fetch_mode
        return statement

    def assemble(self):
        sql = self.SQL_SELECT

        for part in self.__parts:
            sql = getattr(self, '__render_' + part.lower().replace(' ', '_'))(sql)

        return sql

    def __join(self, join_type, table_name, condition, columns=SQL_WILDCARD, schema=None):
        # TODO
        pass

    def __join_using(self, join_type, table_name, condition, columns=SQL_WILDCARD, schema=None):
        # TODO
        pass

    def __unique_correlation(self, name):
        # TODO
        pass

    def __table_cols(self, table_name, columns):
        if isinstance(columns, dict):
            for alias, column in columns:
                self.__table_cols(table_name, ' AS '.join([alias, column]))

            return self

        if isinstance(columns, list) or isinstance(columns, tuple):
            for column in columns:
                if isinstance(column, list) and len(column) == 2:
                    return self.__table_cols(table_name, {column[0]: column[1]})

                return self.__table_cols(table_name, column)

        if isinstance(columns, str):
            parts = re.split(r' as | ', columns, 0, re.IGNORECASE)
            if len(parts) == 1:
                self.__parts[self.COLUMNS][table_name].append(parts)
            elif len(parts) == 2:
                self.__parts[self.COLUMNS][table_name].append({parts[0]: parts[1]})
            else:
                raise ValueError

        return self

    def __where(self, condition, value=None, value_type=None, where_type=SQL_AND):
        # TODO
        pass

    def __render_distinct(self, sql):
        # TODO
        pass

    def __render_columns(self, sql):
        # TODO
        pass

    def __render_from(self, sql):
        # TODO
        pass

    def __render_union(self, sql):
        # TODO
        pass

    def __render_where(self, sql):
        # TODO
        pass

    def __render_group(self, sql):
        # TODO
        pass

    def __render_having(self, sql):
        # TODO
        pass

    def __render_order(self, sql):
        # TODO
        pass

    def __render_limit_offset(self, sql):
        # TODO
        pass

    def __render_for_update(self, sql):
        # TODO
        pass

    def __call__(self, args):
        # TODO
        pass

    def __str__(self):
        # TODO
        pass


class DataObject(ABC):
    # ********************************************
    # Documentation for Abstract Class Variables:
    # ********************************************
    #
    # TABLE_NAME = "my_table"
    # FIELDS = {
    #     'my_instance_variable': {
    #         'field_name': 'my_field_name',
    #         'data_type': 'NULL|INTEGER|REAL|TEXT|BLOB',
    #         'allow_null': True|False,
    #         'default_value': 'my_value'
    #     },
    # }
    # PRIMARY_KEY = ['my_field_name',]
    # TABLE_RELATIONSHIPS = {'table_name': {'fields': [['left_table_field', 'right_table_field'],],}
    # UNIQUE_INDEXES = {
    #     'my_unique_index_name': ['my_field_1', 'my_field_2',],
    # }
    # INDEXES = {
    #     'my_index_name': ['my_field_1', 'my_field_2',],
    # }

    TABLE_NAME = ""
    FIELDS = {}
    PRIMARY_KEY = []
    TABLE_RELATIONSHIPS = {}
    UNIQUE_INDEXES = {}
    INDEXES = {}

    def __init__(self, data_adapter, values=None):
        self.data_adapter = data_adapter
        self.data_adapter.validate_object(self)

        if values is not None:
            pass

    def load(self, primary_key):
        pass

    def get_collection(self):
        return DataObjectCollection(self.data_adapter)

    def save(self):
        pass


class DataObjectCollection(ABC):
    def __init__(self, data_adapter=None):
        self.data_adapter = data_adapter
        self.data_query = data_adapter.get_data_query

        self.current = 0
        self.items = None

        super().__init__()

    def __iter__(self):
        return self

    def next(self):
        self.current += 1
        try:
            return self.items.index(self.current)
        except ValueError:
            raise StopIteration

    def add_field_filter(self, field_filter):
        return self

    def add_field_filters(self, field_filters):
        for field_filter in field_filters:
            self.add_field_filter(field_filter)

        return self

    def add_field(self, field):
        return self

    def add_fields(self, fields):
        for field in fields:
            self.add_field(field)

        return self

    def clear(self):
        pass

    def count(self):
        pass

    def get_items(self):
        pass


class Song(DataObject):
    TABLE_NAME = 'song'

    FIELDS = {
        'id': {'field_name': 'ROWID', 'data_type': 'INTEGER', 'allow_null': False},
        'title': {'field_name': 'name', 'data_type': 'TEXT'},
        'search_title': {'field_name': 'search_name', 'data_type': 'TEXT'},
        'path': {'data_type': 'TEXT'},
        'disc_number': {'data_type': 'TEXT'},
        'track_number': {'data_type': 'TEXT'},
        'play_count': {'data_type': 'INTEGER', 'default_value': 0},
        'artist_id': {'data_type': 'INTEGER'},
        'album_id': {'data_type': 'INTEGER'},
    }

    PRIMARY_KEY = ['ROWID']

    TABLE_RELATIONSHIPS = {
        'artist': {'fields': [['artist_id', 'ROWID']]},
        'album': {'fields': [['album_id', 'ROWID']]},
    }

    UNIQUE_INDEXES = {
        'unq_song_path': ['path'],
    }

    INDEXES = {
        'idx_song_name_artist_id_album_id': ['name', 'artist_id', 'album_id'],
        'idx_song_name_artist_id': ['name', 'artist_id'],
        'idx_song_name_album_id': ['name', 'album_id'],
        'idx_song_name': ['name'],
        'idx_song_search_name': ['search_name'],
        'idx_song_album_id': ['album_id'],
        'idx_song_artist_id': ['artist_id'],
    }

    def __init__(self, data_adapter, song=None):
        super().__init__(data_adapter, song)

    def load(self, song_id):
        pass

    def find(self, conditions):
        pass

    def save(self):
        pass


class Album(DataObject):
    TABLE_NAME = 'album'

    FIELDS = {
        'id': {'field_name': 'ROWID', 'data_type': 'INTEGER', 'allow_null': False},
        'title': {'field_name': 'name', 'data_type': 'TEXT'},
        'search_title': {'field_name': 'search_name', 'data_type': 'TEXT'},
        'play_count': {'data_type': 'INTEGER', 'default_value': 0},
        'artist_id': {'data_type': 'INTEGER'},
    }

    PRIMARY_KEY = ['ROWID']

    TABLE_RELATIONSHIPS = {
        'song': {'fields': [['ROWID', 'album_id']]},
        'artist': {'fields': [['artist_id', 'ROWID']]},
    }

    UNIQUE_INDEXES = {
        'unq_album_name_artist_id': ['name', 'artist_id'],
    }

    INDEXES = {
        'idx_album_name': ['name'],
        'idx_album_search_name': ['search_name'],
        'idx_album_artist_id': ['artist_id'],
    }

    def __init__(self, data_adapter, album=None):
        super().__init__(data_adapter, album)

    def load(self, album_id):
        pass

    def find(self, conditions):
        pass

    def save(self):
        pass


class Artist(DataObject):
    TABLE_NAME = 'artist'

    FIELDS = {
        'id': {'field_name': 'ROWID', 'data_type': 'INTEGER', 'allow_null': False},
        'name': {'data_type': 'TEXT'},
        'search_name': {'data_type': 'TEXT'},
        'play_count': {'data_type': 'INTEGER', 'default_value': 0},
    }

    PRIMARY_KEY = ['ROWID']

    TABLE_RELATIONSHIPS = {
        'song': {'fields': [['ROWID', 'artist_id']]},
        'album': {'fields': [['ROWID', 'artist_id']]},
    }

    UNIQUE_INDEXES = {
        'unq_artist_name': ['name'],
    }

    INDEXES = {
        'idx_artist_search_name': ['search_name'],
    }

    def __init__(self, data_adapter, artist=None):
        super().__init__(data_adapter, artist)

    def load(self, artist_id):
        pass

    def find(self, conditions):
        pass

    def save(self):
        pass
