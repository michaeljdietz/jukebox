import sys
from mutagen.mp4 import MP4, MP4Tags
from .song import Song

class MP4Object(Song):
    def __init__(self, path):
        try:
            data = MP4(str(path)).tags
        except:
            # Song has no ID3 tags, so don't store it in our database as it isn't suitable for voice commands
            return None 

        self.title = str(self.get_title_tag(data))
        self.disc_number = str(self.get_disc_number_tag(data))
        self.track_number = str(self.get_track_number_tag(data))
        self.album_name = str(self.get_album_tag(data))
        self.artist_name = str(self.get_artist_tag(data))

    def get_title_tag(self, song):
        if '\xa9nam' in song:
            return song['\xa9nam'][0]

        return ''

    def get_disc_number_tag(self, song):
        if 'disk' in song:
            return song['disk'][0][0]

        return ''

    def get_track_number_tag(self, song):
        if 'trkn' in song:
            return song['trkn'][0][0]

        return ''

    def get_album_tag(self, song):
        if '\xa9alb' in song:
            return song['\xa9alb'][0]

        return ''

    def get_artist_tag(self, song):
        if '\xa9ART' in song:
            return song['\xa9ART'][0]

        if '\xa9wrt' in song:
            return song['\xa9wrt'][0]

        return ''
