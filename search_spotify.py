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

def verify_album_match(album_result, true_album, true_artist, true_year=None):
    pass

def spotify_worker(args):
    client_id, client_secret, df = args
    app_token  = tk.request_client_token(client_id, client_secret)
    spotify = tk.Spotify(app_token)

    for pitchfork_id, artist, album, year in df.itertuples(index=False, name=None):
        q = get_query(artist, album, year)
        
        albums, = spotify.search(q, types=('album',))
        
        # If no results found and multiple artists (with Tyler edge case)
        if albums.total == 0 and artist.find(',') != -1:
            # Loop over artist names and search
            pass
        # Results found
        else:
            for album in albums.items:
                
        
        album_info_dict = {
            'pitchfork_id': pitchfork_id,
            'artist': artist,
            'album': album,
            'year': year,
        }
        
        for curr_item in results['albums']['items']:
            curr_artists = []
            for x in curr_item['artists']:
                pass

def get_spotify_worker_data(df):
    

def spotify_manager(df):
    worker_data = get_spotify_worker_data(df)
    with Pool(2) as p:


def call_spotipy_worker(args):
    SPOTIFY_CLIENT_ID, SPOTIFY_SECRET_KEY, df = args
    scope = 'user-library-read'
    sp = spotipy.Spotify(
        auth_manager = SpotifyOAuth(
            scope=scope, 
            client_id=SPOTIFY_CLIENT_ID, 
            client_secret=SPOTIFY_SECRET_KEY, 
            redirect_uri='https://stanfordflip.org'
            )
        )
    
    album_result_list = []

    found = 0
    not_found = 0

    for pitchfork_id, artist, album, year in df.itertuples(index=False, name=None):
        q = get_query(artist, album, year)
        results = sp.search(q,type='album')
        
        if results['albums']['total'] == 0:
            not_found += 1
            continue
        else:
            found += 1
        
        album_info_dict = {
            'pitchfork_id': pitchfork_id,
            'artist': artist,
            'album': album,
            'year': year,
        }
        
        for curr_item in results['albums']['items']:
            curr_artists = []
            for x in curr_item['artists']:
                pass

        print(f"ID {pitchfork_id}")
    
    print(found)
    print(not_found)

def call_spotipy_manager(df):
    scope = 'user-library-read'
    sp = spotipy.Spotify(
        auth_manager = SpotifyOAuth(
            scope=scope, 
            client_id=SPOTIFY_CLIENT_ID, 
            client_secret=SPOTIFY_SECRET_KEY, 
            redirect_uri='https://stanfordflip.org'
            )
        )
    
    album_result_list = []

    found = 0
    not_found = 0

    for pitchfork_id, artist, album, year in df.itertuples(index=False, name=None):
        q = get_query(artist, album, year)
        results = sp.search(q,type='album')
        
        if results['albums']['total'] == 0:
            not_found += 1
            continue
        else:
            found += 1
        
        album_info_dict = {
            'pitchfork_id': pitchfork_id,
            'artist': artist,
            'album': album,
            'year': year,
        }
        
        for curr_item in results['albums']['items']:
            curr_artists = []

        print(f"ID {pitchfork_id}")
    
    print(found)
    print(not_found)


if __name__ == '__main__':
    df = parse_pitchfork_csv('bungus.csv')
    call_spotipy(df)