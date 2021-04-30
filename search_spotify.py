from multiprocessing import Pool
import pandas as pd
from pprint import pprint
import tekore as tk

def parse_pitchfork_csv(filename):
    df = pd.read_csv(filename)
    df = df[['pitchfork_id', 'artist', 'album', 'release_year']]
    return df

def get_query(artist, album, year):
    if pd.isna(year):
        q = f'album:{album} artist:{artist}'
    else:
        q = f'album:{album} artist:{artist} year:{year}'
    q = q.replace('“','').replace('”','')
    return q

def verify_album_match(album, wanted_album, wanted_artist, wanted_year=None):
    album_name = album.name
    if wanted_album != album_name:
        return False
    if wanted_year != None:
        release_year = album.release_date.split('-')[0]
        if release_year != wanted_year:
            return False
    artists_found = album.artists
    if len(artists_found) == 1:
        if artists_found[0].name != wanted_artist:
            return False
    # if spotify has multiple artists listed for an album
    else:
        #wanted artist: "Kanye West, Jay-Z"
        #artists_found = ["Kanye West", "Jay-Z", "Tyler, the Creator"]
        # potentially need to change this 
        for artist in artists_found:
            artist_name = artist.name
            if artist_name not in wanted_artist:
                return False
    return True


def build_album_dict(album, pitchfork_id, artist, search_album, year):
    artist_ids = ','.join([x.id for x in album.artists])
    album_info_dict = {
        'album_id': album.id,
        'artist_ids': artist_ids,
        'pitchfork_id': pitchfork_id,
        'artist': artist,
        'album': search_album,
        'year': year
    }
    return album_info_dict


def spotify_worker(args):
    client_id, client_secret, df = args
    app_token  = tk.request_client_token(client_id, client_secret)
    spotify = tk.Spotify(app_token)
    results = []
    # loop through each row in pitchfork data to find matching album on spotify 
    for pitchfork_id, artist, search_album, year in df.itertuples(index=False, name=None):
        q = get_query(artist, search_album, year)
        albums, = spotify.search(q, types=('album',)
        # If no results found and multiple artists (with Tyler edge case)
        if albums.total == 0 and artist.find(', ') != -1:
            # Loop over artist names and redo search
            query_artists = artist.split(', ')
            for query_artist in query_artists:
                new_query = get_query(query_artist, album, year)
                albums, = spotify.search(new_query, types=('album',))
                for album in albums:
                    if verify_album_match(album_result, search_album, artist, year):
                        results.append(build_album_dict(album, pitchfork_id, artist, search_album, year))
        else: 
            for album in albums.items:
                if verify_album_match(album_result, search_album, artist, year):
                    results.append(build_album_dict(album, pitchfork_id, artist, search_album, year))


def get_spotify_worker_data(df):
    

def spotify_manager(df):
    worker_data = get_spotify_worker_data(df)
    with Pool(2) as p:

if __name__ == '__main__':
    df = parse_pitchfork_csv('bungus.csv')
    call_spotipy(df)