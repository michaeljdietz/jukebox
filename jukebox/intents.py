import sys
from .app import ask, app, library
from . import grammar
from .playback import Playback
from flask_ask import statement, audio, current_stream, context, question, request

playback = Playback([])
base_url = 'https://michaeljdietz.me'

def prepare_ssml(input_text):
    return input_text.replace("&", " number ")

@ask.on_playback_nearly_finished()
def nearly_finished():
    if playback.up_next:
        next_stream = playback.up_next
        return autio().enqueue(next_stream)

@ask.on_playback_finished()
def play_back_finished():
    if playback.up_next:
        playback.step()
    else:
        return None

@ask.intent("JukeboxPlayAlbumByArtist")
def play_album_by_artist(album_name, artist_name):
    if album_name is None:
        return

    album_id = library.database.get_album_id_by_album_artist(album_name, artist_name, False, False)
 
    if album_id is None:
        return statement("I could not find that album in your library").simple_card("Find album %s", "Could not find any matching albums in your library")

    real_artist_name = library.database.get_artist_name_by_album_id(album_id)
    real_album_name = library.database.get_album_name_by_album_id(album_id)
    songs = library.database.get_songs_by_album_id(album_id)

    playback = Playback([base_url + '/songs/' + str(song) for song in songs])
    stream_url = playback.start()

    speech_text = "Playing the album %s by %s on your jukebox" % (real_album_name, real_artist_name)

    return audio(speech_text).play(stream_url)

@ask.intent('AMAZON.NextIntent')
def next_song():
    if playback.up_next:
        next_stream = playback.step()
        return audio().play(next_stream)

@ask.intent('AMAZON.PreviousIntent')
def previous_song():
    if playback.previous:
        prev_stream = playback.step_back()
        return audio().play(prev_stream)

@ask.intent('AMAZON.StartOverIntent')
def restart_track():
    if playback.current:
        return audio().play(playback.current, offset=0)

@ask.on_playback_started()
def started(offset, token, url):
    print('Started autio stream for track {}'.format(playback.current_position))

@ask.on_playback_stopped()
def stopped(offset, token):
    print('Stopped audio stream for track {}'.format(playback.current_position))

@ask.intent('AMAZON.PauseIntent')
def pause():
    return audio().stop()

@ask.intent('AMAZON.ResumeIntent')
def resume():
    return audio().resume() 

@ask.session_ended
def session_ended():
    return "{}", 200

@ask.intent("JukeboxListAlbumsByArtist")
def list_albums_by_artist(artist_name):
    if artist_name is None:
        return

    results = library.database.get_all_albums_by_artist(artist_name)
    real_artist_name = library.database.get_artist_name(artist_name)

    if real_artist_name is None:
        real_artist_name = artist_name

    speech_text = "I have found %s albums by %s in your library: " % (str(len(results)), real_artist_name)
    if (len(results) > 1):
        speech_text += ", ".join(results[:-1])
        speech_text += ", and %s" % results[-1]
    elif len(results) == 1:
        speech_text += results[0]

    print('Album search results: %s' % results, file=sys.stderr)

    return statement(prepare_ssml(speech_text)).simple_card("List all albums by %s" % artist_name, speech_text)
