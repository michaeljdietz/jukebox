import sys
import os
import threading
import time
from .database import Database
from pathlib import Path
from queue import Queue
from .mp3 import MP3Object
from .mp4 import MP4Object

class Library:
    SQL_BUFFER_SIZE=100000
    MAX_THREAD_COUNT=100
    THREAD_WAIT=0.25

    def __init__(self, library_path):
        self.library_path = library_path
        self.database = Database()
        # TODO: Build library if the database was just created
        #self.build_library()

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
