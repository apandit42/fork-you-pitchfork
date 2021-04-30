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
    album_name = album_result.name 
    if album_result != true_album:
        return False
    if true_year != None:
        release_year = album_result.release_date.split('_')[0]
        if release_year != true_year:
            return False
    artists = album_result.artists
    #if multiple artists, make sure that they are in the string ... check for commas  
    if true_artist.find(', ') != -1:
        search_artists = true_artist.split(', ')
        for artist in search_artists:
            # doesn't take into account whether there are extra artists or not.... 
            if artist not in artists:
                return False 
    return True


def spotify_worker(args):
    client_id, client_secret, df = args
    app_token  = tk.request_client_token(client_id, client_secret)
    spotify = tk.Spotify(app_token)

    # loop through each row in pitchfork data to find matching album on spotify 
    for pitchfork_id, artist, search_album, year in df.itertuples(index=False, name=None):
        q = get_query(artist, album, year)
        albums, = spotify.search(q, types=('album',))
        
        # If no results found and multiple artists (with Tyler edge case)
        if albums.total == 0 and artist.find(', ') != -1:
            # Loop over artist names and redo search
            query_artists = artist.split(', ')
            for i in range(len(query_artists)):
                first_artist = query_artists[i]
                other_artists = ', '
                for j in range(len(query_artists)):
                    if j != i:
                        other_artists += query_artists[j]
                    if j != len(query_artists) - 1:
                        other_artists += ', '
                rearranged_artist = first_artist + other_artists
                new_query = get_query(rearranged_artist, album, year)
                albums, = spotify.search(new_query, types=('album',))
                for album in albums.items:
                    if verify_album_match(album_result, search_album, artist, year):
                        # if correct album is found, add to our data structure (list of dicts?)
                        album_id = album.id 
                        #wait how are we taking care of multiple artists? row for each? i forgot... 
                        break
            pass
        # Results found
        else:
            # verify that the wanted album is actually found 
            for album in albums.items:
                if verify_album_match(album_result, search_album, artist, year):
                    # if correct album is found, add to our data structure (list of dicts?) 
                    break
                # check if the artist name is correct - if multiple artists, check that they are each in the original artist string 
                
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