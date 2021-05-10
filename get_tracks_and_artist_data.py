import pandas as pd
from pprint import pprint
import tekore as tk

def get_tracks_and_artist_data:
    albums_found = pd.read_csv('spotify_album_and_artist_ids.csv')
    grouped = albums_found.groupby('pitchfork_id')
    tracks = []
    artists = []
    num_track = 0
    num_artist = 0
    for group in grouped:
        chosen_album_id, chosen_artist_id, chosen_album = find_best_match(group)
        # construct a list of lists, each internal list w 50 ids 
        for track in chosen_album_id.tracks.items: 
            tracks[num_track // 50].append(track.id)
            num_track += 1
        for artist in chosen_album.artists:
            artists[num_artist // 50].append(artist.id)
            num_artist += 1
    get_artist_data(artists)
    get_track_data(tracks)


def get_track_data(lists):
    for tracks_ids in lists:
        tracks_data = spotify.tracks_audio_features(tracks_ids)
    

def get_artist_data(lists):
    for artists_ids in lists:
        artists_data = spotify.artists(artists_ids)
        


def find_best_match(group):
    if len(group) == 1:
        return group.album_id, group[0].artist_id 
    chosen_album_id, chosen_artist_id, chosen_album = None 
    top_popularity = 0
    found_explicit = False 
    for pitchfork_id, album, album_id, artist, artist_id in group:
            spotify_album = Spotify.album(album_id)
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
            if chosen_album_id == None:
                chosen_album_id = album_id
                chosen_artist_id = artist_id
                top_popularity = popularity
                chosen_album = Spotify.album(album_id)
            elif popularity > last_popularity:
                chosen_album_id = album_id
                chosen_artist_id = artist_id
                top_popularity = popularity
                chosen_album = Spotify.album(album_id)
    # save the popularity somewhere! 
    return chosen_album_id, chosen_artist_id, chosen_album