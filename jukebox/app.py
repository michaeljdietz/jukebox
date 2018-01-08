from flask import Flask, Response, request, send_file
from werkzeug.datastructures import Headers
from flask_ask import Ask
from time import time
from re import findall
import sys
import os

def init():
    global app, ask, library

    library_path = '/home/plex/shares/k/Music/Music'

    app = Flask(__name__)
    ask = Ask(app, '/')

    from .library import Library

    print('Loading library...', file=sys.stderr)
    library = Library(library_path)
    print('Done loading library!', file=sys.stderr)

    from . import selection
    from . import playback
    from . import intents

    @app.route('/songs/<song_id>')
    def get_song_stream(song_id):
        path = library.database.get_song_path_by_song_id(song_id)
        print("Received request:\n %s" % request.headers, file=sys.stderr)
        print("Playing %s" % path,file=sys.stderr)
        def generate():
            with open(path, "rb") as fmp3:
                data = fmp3.read(1024)
                while data:
                    yield data
                    data = fmp3.read(1024)
        return Response(generate(), mimetype="audio/mpeg")
