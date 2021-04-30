from multiprocessing import Pool
import pandas as pd
from pprint import pprint
import tekore as tk
from secrets import *


# Build the query string for spotify search calls
def get_query(artist, album, year):
    if pd.isna(year):
        q = f'album:{album} artist:{artist}'
    else:
        q = f'album:{album} artist:{artist} year:{year}'
    q = q.replace('“','').replace('”','')
    return q


# Verify that album found from spotify search matches actual pitchfork album information
def verify_album_match(album_result, true_album, true_artist, true_year=None):
    album_name = album_result.name 
    if album_result != true_album:
        return False
    if wanted_year != None:
        release_year = album.release_date.split('-')[0]
        if release_year != wanted_year:
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


# Worker function for searching spotify for albums
def spotify_worker(args):
    # Split up the provided arguments
    client_id, client_secret, df = args

    # Setup tk Spotify session
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
                        # if correct album is found, add to our data structure (list of dicts?)
                        album_id = album.id 
                        #wait how are we taking care of multiple artists? row for each? i forgot... 
                        results.append(build_album_dict(album, pitchfork_id, artist, search_album, year))
        else: 
            for album in albums.items:
                if verify_album_match(album_result, search_album, artist, year):
                    # if correct album is found, add to our data structure (list of dicts?) 
                    break
        

# Packages all pitchfork data for the manager and multiprocessing calls
def get_spotify_worker_data(df):
    # Get total rows
    total_rows = df.shape[0]
    
    # Split into 3 chunks
    chunk_0 = df.iloc[0:int(total_rows/3), :]
    chunk_1 = df.iloc[int(total_rows * 1/3):int(total_rows * 2/3), :]
    chunk_2 = df.iloc[int(total_rows * 2/3):, :]

    # Now load client_id and secret_key into chunk tuples
    worker_data_package = [
        (chunk_0, AYUSH_SPOTIFY_CLIENT_ID, AYUSH_SPOTIFY_SECRET_KEY),
        (chunk_1, KIMBERLY_SPOTIFY_CLIENT_ID, KIMBERLY_SPOTIFY_SECRET_KEY),
        (chunk_2, COLIN_SPOTIFY_CLIENT_ID, COLIN_SPOTIFY_SECRET_KEY),
    ]

    # Return chunked data
    return worker_data_package


def spotify_manager(df):
    worker_data = get_spotify_worker_data(df)
    with Pool(2) as p:
    

    return worker_data_package


# Manager function for searching spotify for all albums
def spotify_manager(df):
    # Grab worker data in organized way
    worker_data = get_spotify_worker_data(df)
    
    # 3 keys so hard coded in
    WORKER_THREADS = 3

    # Send data to the pool workers
    with Pool(WORKER_THREADS) as p:
        worker_output = p.map(spotify_worker, worker_data)
    
    # Now unpack everything
    output_list = [y for x in worker_output for y in x]

    # Now convert to dataframe
    output_df = pd.DataFrame(output_list)

    # Write it out
    output_df.to_csv('spotify_album_ids.csv')


if __name__ == '__main__':
    df = pd.read_csv('pitchfork_core.csv')
    spotify_manager(df)