import pandas as pd
from pprint import pprint
import tekore as tk
import pickle
from pathlib import Path
from keys import *

def get_tracks_and_artist_data:
    albums_found = pd.read_csv('THANOS.csv')
    grouped = albums_found.groupby('pitchfork_id')
    tracks = []
    artists = []
    num_track = 0
    num_artist = 0
    album_results = []
    for group in grouped:
        chosen_album = find_best_match(group)
        # construct a list of lists, each internal list w 50 ids 
        dictionary['tracks'] = chosen_album.tracks.items
        for track in chosen_album.tracks.items: 
            if len(tracks) // 50 == 0:
                tracks.append([])
            tracks[num_track // 50].append(track.id)
            num_track += 1
       dictionary['artists'] =  '|'.join([x.name for x in chosen_album.artists])
        for artist in chosen_album.artists:
            if len(artists) // 50 == 0:
                artists.append([])
            artists[num_artist // 50].append(artist.id)
            num_artist += 1
        album_results.append[dictionary]
    get_artist_data(artists)
    get_track_data(tracks)


def get_track_data(lists):
    results = []
    for tracks_ids in lists:
        tracks_data = spotify.tracks(tracks_ids)
         for track_data in tracks_data:
            dictionary = {
                'duration_ms': track_data.duration_ms,
                'popularity': track_data.duration_ms
            }
        results.append(dictionary)
    return results

def get_track_audio_data(lists):
    results = []
    for track_ids in lists:
        tracks_audio_data = spotify.tracks_audio_features(tracks_ids)
        for track_audio_data in tracks_audio_data:
            dictionary = {
                'acousticness': track_audio_data.acousticness,
                'danceability': track_audio_data.danceability,
                'energy': track_audio_data.energy,
                'instrumentalness': track_audio_data.instrumentalness,
                'key': track_audio_data.key,
                'liveness': track_audio_data.liveness,
                'loudness': track_audio_data.loudness,
                'mode': track_audio_data.mode,
                'speechiness': track_audio_data.speechiness,
                'tempo': track_audio_data.tempo,
                'time_signature': track_audio_data.time_signature,
                'valence': track_audio_data.valence
            }
        results.append(dictionary)
    return results
        

def get_artist_data(lists):
    artists = []
    for artists_ids in lists:
        artists_data = spotify.artists(artists_ids)
        for artist_data in artists_data:
            dictionary = {
                'pitchfork_id': pitchfork_id,
                'num_followers': artist_data.followers.total,
                'popularity': artist_data.popularity,
                'name': artist_data.name,
                'genres': artist_data.genres
            }
            artists.append[dictionary]
    return artists

def find_best_match(group):
    if len(group) == 1:
        return spotify.album(group[0].album_id)
    chosen_album = None 
    top_popularity = 0
    found_explicit = False 
    album_ids = []
    for album_id in group:
        album_ids.append(album_id)
    spotify_album_data = spotify.albums(album_ids)
    for spotify_album in spotify_album_data:
        popularity = spotify_album.popularity
        explicit = False
        for track in spotify_album.tracks.items:
            if track.explicit:
                explicit = True
                found_explicit = True
                break
        # if this is the first result we have seen
        if found_explicit and not explicit:
            continue 
        if chosen_album == None:
            top_popularity = popularity
            chosen_album = spotify_album
        elif popularity > top_popularity:
            top_popularity = popularity
            chosen_album = spotify_album
    # save the popularity, name, label, total_tracks release_date somewhere! 
    dictionary = {
        'pitchfork_id': pitchfork_id,
        'album_name': chosen_album.name,
        'track_count': chosen_album.total_tracks,
        'release_date': chosen_album.release_date
        'popularity': chosen_album.popularity
    }
    return chosen_album, dictionary


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Search Spotify')
    parser.add_argument('--ap', help='Ayush key', action='store_true')
    parser.add_argument('--ck', help='Colin key', action='store_true')
    parser.add_argument('--kb', help='Kimberly key', action='store_true')
    parser.add_argument('--ALBUMMODE', help='Run group albums', action='store_true')
    parser.add_argument('src', help='Source CSV')
    parser.add_argument('dest', help='Destination CSV')
    args = parser.parse_args()
    if args.ap:
        client_id = AYUSH_SPOTIFY_CLIENT_ID
        client_secret = AYUSH_SPOTIFY_SECRET_KEY
    elif args.ck:
        client_id = COLIN_SPOTIFY_CLIENT_ID
        client_secret = COLIN_SPOTIFY_SECRET_KEY
    else:
        client_id = KIMBERLY_SPOTIFY_CLIENT_ID
        client_secret = KIMBERLY_SPOTIFY_SECRET_KEY