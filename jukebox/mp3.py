import sys
from mutagen.id3 import ID3
from .song import Song

class MP3Object(Song):
    def __init__(self, path):
        try:
            data = ID3(str(path))
        except:
            # Song has no ID3 tags, so don't store it in our database as it isn't suitable for voice commands
            return None

        self.title = str(self.get_title_tag(data))
        self.disc_number = str(self.get_disc_number_tag(data))
        self.track_number = str(self.get_track_number_tag(data))
        self.album_name = str(self.get_album_tag(data))
        self.artist_name = str(self.get_artist_tag(data))

    def get_title_tag(self, song):
        if 'TIT2' in song:
            return song['TIT2']

        return ''

    def get_disc_number_tag(self, song):
        if 'TPOS' in song:
            return song['TPOS']

        return ''

    def get_track_number_tag(self, song):
        if 'TRCK' in song:
            return song['TRCK']

        return ''

    def get_album_tag(self, song):
        if 'TALB' in song:
            return song['TALB']

        return ''

    def get_artist_tag(self, song):
        if 'TPE2' in song:
            return song['TPE2']

        if 'TPE1' in song:
            return song['TPE1']

        return ''
