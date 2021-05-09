import pandas as pd
from pprint import pprint
import tekore as tk

def get_tracks:
    albums_found = pd.read_csv('spotify_album_and_artist_ids.csv')
    grouped = albums_found.groupby('pitchfork_id')
    for group in grouped:
        chosen_album_id, chosen_artist_id = find_best_match(group)
        get_track_data(chosen_album_id)
        get_artist_data(chosen_artist_id)
        


def find_best_match(group):
    if len(group) == 1:
        return group.album_id, group[0].artist_id 
    chosen_album_id, chosen_artist_id = None 
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
            elif popularity > last_popularity:
                chosen_album_id = album_id
                chosen_artist_id = artist_id
                top_popularity = popularity
    return chosen_album_id, chosen_artist_id