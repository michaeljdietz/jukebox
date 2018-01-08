import sys
import threading
import time
import sqlite3
from queue import Queue
from types import SimpleNamespace
from . import grammar
from .mp3 import MP3Object
from .mp4 import MP4Object

class Database:
    DB_PATH='database/library.db'

    def __init__(self):
        self.song_queue = Queue()
        self.song_count = 0

        try:
            open(self.DB_PATH)
        except:
            print('Creating database... ', file=sys.stderr, end='')
            sys.stderr.flush()

            self.create_database()

            print('Done', file=sys.stderr)
            sys.stderr.flush()

    def __del__(self):
        if hasattr(self, 'connection'):
            self.connection.close()

    def get_connection(self):
        if hasattr(self, 'connection'):
            return self.connection

        self.connection = sqlite3.connect(self.DB_PATH)

        return self.connection

    def get_cursor(self):
        if hasattr(self, 'cursor'):
            return self.cursor

        self.cursor = self.get_connection().cursor()
        return self.cursor

    def create_database(self):
        self.create_table_artists()
        self.create_table_albums()
        self.create_table_songs()

    def create_table_artists(self):
        self.get_connection().execute('''
            CREATE TABLE artist (
                ROWID INTEGER NOT NULL,
                name TEXT,
                search_name TEXT,
                play_count INTEGER DEFAULT 0,
                PRIMARY KEY(ROWID)
            );
        ''')
        self.get_connection().execute('''CREATE UNIQUE INDEX unq_artist_name ON artist(name)''')
        self.get_connection().execute('''CREATE INDEX idx_artist_search_name ON artist(search_name)''')
        self.get_connection().commit()
        return self

    def create_table_albums(self):
        self.get_cursor().execute('''
            CREATE TABLE album (
                ROWID INTEGER NOT NULL,
                name TEXT,
                search_name TEXT,
                artist_id INTEGER,
                play_count INTEGER DEFAULT 0,
                PRIMARY KEY(ROWID)
             );
        ''')
        self.get_connection().execute('''CREATE UNIQUE INDEX unq_album_name_artist_id ON album(name, artist_id)''')
        self.get_connection().execute('''CREATE INDEX idx_album_name ON album(name)''')
        self.get_connection().execute('''CREATE INDEX idx_album_search_name ON album(search_name)''')
        self.get_connection().execute('''CREATE INDEX idx_album_artist_id ON album(artist_id)''')
        self.get_connection().commit()
        return self

    def create_table_songs(self):
        self.get_connection().execute('''
            CREATE TABLE song (
                ROWID INTEGER NOT NULL,
                name TEXT,
                search_name TEXT,
                path TEXT,
                disc_number TEXT,
                track_number TEXT,
                artist_id INTEGER,
                album_id INTEGER,
                play_count INTEGER DEFAULT 0,
                PRIMARY KEY(ROWID)
            );
        ''')
        self.get_connection().execute('''CREATE UNIQUE INDEX unq_song_path ON song(path)''')
        self.get_connection().execute('''CREATE INDEX idx_song_name_artist_id_album_id ON song(name, artist_id, album_id)''')
        self.get_connection().execute('''CREATE INDEX idx_song_name_artist_id ON song(name, artist_id)''')
        self.get_connection().execute('''CREATE INDEX idx_song_name_album_id ON song(name, album_id)''')
        self.get_connection().execute('''CREATE INDEX idx_song_name ON song(name)''')
        self.get_connection().execute('''CREATE INDEX idx_song_search_name ON song(search_name)''')
        self.get_connection().execute('''CREATE INDEX idx_song_album_id ON song(album_id)''')
        self.get_connection().execute('''CREATE INDEX idx_song_artist_id ON song(artist_id)''')
        self.get_connection().commit()
        return self

    def flush_song_queue(self):
        songs_processed = self.song_count
        self.song_count += self.song_queue.qsize()
        rows = []

        current_artist_name = None
        current_album_name = None

        while not self.song_queue.empty():
            songs_processed += 1
            print("\rProcessing song %s / %s" % (songs_processed, self.song_count), end='', file=sys.stderr)
            sys.stderr.flush()

            song = self.song_queue.get()

            if not hasattr(song, 'title') or self.file_path_exists_in_database(song.path):
                continue

            try:
                song.artist_search_name = grammar.strip_articles(song.artist_name)
                song.album_search_name = grammar.strip_articles(song.album_name)
                song.search_title = grammar.strip_articles(song.title)
            except:
                print('ERROR getting info for %s' % song.title, file=sys.stderr)

            if song.artist_name != current_artist_name:
                artist_id = self.get_artist_id(song)
                current_artist_name = song.artist_name
                
            song.artist_id = artist_id

            if song.album_name != current_album_name:
                album_id = self.get_album_id(song)
                current_album_name = song.album_name

            song.album_id = album_id
            rows.append(self.song_to_array(song))

        self.insert_song_rows(rows)

        return self

    def song_to_array(self, song):
        return [song.title, song.search_title, song.path, song.disc_number, song.track_number, song.artist_id, song.album_id]

    def process_song(self, song):
        song.artist_id = self.get_artist_id(song)
        song.album_id = self.get_album_id(song)
        song.song_id = self.get_song_id(song)

        return self

    def get_artist_id(self, song):
        results = self.get_connection().execute(
            "SELECT ROWID FROM artist WHERE artist.name = ?",
            (song.artist_name,)).fetchall()

        if results is not None and len(results) > 0:
            return int(results[0][0])

        return self.insert_artist(song)

    def insert_artist(self, song):
        if song.artist_name == "Radiohead":
            print("Adding artist %s" % song.artist_name,file=sys.stderr)

        cursor = self.get_connection().execute("INSERT INTO artist (name, search_name) VALUES (?, ?)", (song.artist_name, song.artist_search_name))
        self.get_connection().commit()

        return cursor.lastrowid

    def get_album_id(self, song):
        results = self.get_connection().execute(
            "SELECT ROWID FROM album WHERE album.name = ? AND artist_id = ?",
            (song.album_name, song.artist_id,)).fetchall()

        if results is not None and len(results) > 0:
            return int(results[0][0])

        return self.insert_album(song)

    def insert_album(self, song):
        if song.artist_name == "Radiohead":
            print("Adding album %s" % song.album_name,file=sys.stderr)

        cursor = self.get_connection().execute("INSERT INTO album (name, search_name, artist_id) VALUES (?, ?, ?)", (song.album_name, song.album_search_name, song.artist_id))
        self.get_connection().commit()
        return cursor.lastrowid

    def get_song_id(self, song):
        results = self.get_connection().execute(
            "SELECT ROWID FROM song WHERE path = ?",
            (song.path,)).fetchall()

        if results is not None and len(results) > 0:
            return int(results[0][0])

        return self.insert_song(song)

    def insert_song(self, song):
        self.get_connection().execute('''INSERT INTO song (name, search_name, path, disc_number, track_number, artist_id, album_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)''', (song.title, song.search_title, song.path, song.disc_number, song.track_number, song.artist_id, song.album_id))
        self.get_connection().commit()
        return self.get_cursor().lastrowid

    def insert_song_rows(self, rows):
        sql = "INSERT INTO song (name, search_name, path, disc_number, track_number, artist_id, album_id) VALUES (?, ?, ?, ?, ?, ?, ?) "

        try:
            cursor = self.get_connection().executemany(sql, rows)
            self.get_connection().commit()           
        except sqlite3.IntegrityError:
            print('Integrity constraint error, rows dump:', file=sys.stderr)
            print(rows, file=sys.stderr)
            sys.exit('Fatal error')
        
    def file_path_exists_in_database(self, path):
        cursor = self.get_connection().execute(
            " SELECT count(*) FROM song WHERE song.path = ?",
            (path,))
        result = cursor.fetchone()[0]
        
        return result > 0

    def get_artist_name(self, artist_search_name, fuzzy = False):
        if artist_search_name is None:
            return None

        prepared_search_name = grammar.strip_articles(artist_search_name)

        if not fuzzy:
            operator = "="
        else:
            operator = "like"
            prepared_search_name = "%" + prepared_search_name

        cursor = self.get_connection().execute("SELECT name FROM artist WHERE search_name %s ?" % operator, (prepared_search_name,))
        results = cursor.fetchall()
        if len(results) > 1:
            return None
        elif len(results) == 0 and fuzzy == False:
            return self.get_artist_name(artist_search_name, True)
        elif len(results) == 0:
            return None

        return results[0][0]

    def get_artist_name_by_album_id(self, album_id):
        cursor = self.get_connection().execute("SELECT artist.name FROM artist JOIN album ON artist.ROWID = album.artist_id WHERE album.ROWID = ?", (album_id,))
        return cursor.fetchone()[0]

    def get_album_name_by_album_id(self, album_id):
        cursor = self.get_connection().execute("SELECT name FROM album WHERE ROWID = ?", (album_id,))
        return cursor.fetchone()[0]

    def get_album_id_by_album_artist(self, album_name, artist_name = None, fuzzy_artist = False, fuzzy_album = False):
        if album_name is None:
            return None

        sql = "SELECT album.ROWID FROM album "
        and_where = []
        parameters = []

        if artist_name is not None:
            search_artist_name = grammar.strip_articles(artist_name)
            if not fuzzy_artist:
                operator = "="
            else:
                operator = "like"
                search_artist_name = "%" + search_artist_name + "%"

            sql += "JOIN artist ON album.artist_id = artist.ROWID "
            and_where.append("artist.search_name %s ?" % operator)
            parameters.append(search_artist_name)
        
        search_album_name = grammar.strip_articles(album_name)

        if not fuzzy_album:
            operator = "="
        else:
            operator = "like"
            sql_album_name = "%" + search_album_name + "%" 
    
        and_where.append("album.search_name %s ?" % operator)
        parameters.append(search_album_name)

        sql += "WHERE " + " AND ".join(and_where)
        cursor = self.get_connection().execute(sql, parameters)
        results = cursor.fetchall()

        if len(results) == 0 and fuzzy_artist == False and artist_name is not None:
            return self.get_album_id_by_album_artist(album_name, artist_name, True)

        if len(results) == 0 and fuzzy_album == False:
            return self.get_album_id_by_album_artist(album_name, artist_name, True, True)
        
        if len(results) == 0:
            print("No results found: %s" % results, file=sys.stderr)
            return None

        return results[0][0]

    def get_song_path_by_song_id(self, song_id):
        cursor = self.get_connection().execute("SELECT path FROM song WHERE ROWID = ?", (song_id,))
        result = cursor.fetchone()
        
        return result[0]

    def get_songs_by_album_id(self, album_id):
        cursor = self.get_connection().execute("SELECT ROWID FROM song WHERE album_id = ? ORDER BY disc_number ASC, track_number ASC", (album_id,))
        results = cursor.fetchall()
    
        if len(results) == 0:
            return None

        return [result[0] for result in results]
    

    def get_all_albums_by_artist(self, artist_name, fuzzy = False):
        search_artist_name = grammar.strip_articles(artist_name)

        if not fuzzy:
            operator = "="
        else:
            operator = "like"
            search_artist_name = "%" + search_artist_name + "%"

        print("searching db...", file=sys.stderr)
        cursor = self.get_connection().execute(
            "SELECT album.name FROM album INNER JOIN artist ON album.artist_id = artist.ROWID WHERE artist.search_name %s ?" % operator,
            (search_artist_name,))
        results = cursor.fetchall()
        print("done with search!", file=sys.stderr)

        if len(results) == 0 and fuzzy == False:
            return self.get_all_albums_by_artist(artist_name, True)

        if len(results) == 0:
            return None

        return [result[0] for result in results]
