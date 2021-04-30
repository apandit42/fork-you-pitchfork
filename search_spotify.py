import multiprocessing as mp
import pandas as pd
from pprint import pprint
import tekore as tk
import keys


# Build the query string for spotify search calls
def get_query(artist, album, year=None):
    if year is None:
        q = f'album:{album} artist:{artist}'
    else:
        q = f'album:{album} artist:{artist} year:{year}'
    q = q.replace('“','').replace('”','')
    return q


# Verify that the albums match the pitchfork album name, artist, and year
def verify_album_match(album, wanted_album, wanted_artist, wanted_year=None):
    if wanted_album != album.name:
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


# Build each album dict for the list
def build_album_dict(album, pitchfork_id, artist, album_name, year):
    artist_id = ','.join([x.id for x in album.artists])
    album_info_dict = {
        'pitchfork_id': pitchfork_id,
        'album': album_name,
        'album_id': album.id,
        'artist': artist,
        'artist_id': artist_id,
        'year': year,
    }
    return album_info_dict


# Spotify worker threads
def spotify_worker(args, ret_list):
    # Split up the provided arguments
    data, client_id, client_secret = args

    # Setup tk Spotify session
    app_token  = tk.request_client_token(client_id, client_secret)
    spotify = tk.Spotify(app_token, sender=tk.RetryingSender(retries=1000))

    # Convert data to dataframe
    df = pd.DataFrame(data)

    # Begin the results list which will hold all the row dicts
    results = []

    # Counter of found results + not found results
    found_results = 0
    not_found_results = 0

    # loop through each row in pitchfork data to find matching album on spotify 
    for pitchfork_id, artist, album_name, year in df.itertuples(index=False, name=None):
        # Check if year NaN, and if so, set to none
        if pd.isna(year):
            year = None
        
        # Confirm run printing
        print(f'ID {pitchfork_id} w/ {album_name} by {artist} ({year})...')
        
        # Albums to verify before building
        candidate_matches = []

        # Boolean flag if this album has been found
        album_found = False

        # Build search query & get initial search results
        q = get_query(artist, album_name, year)
        albums, = spotify.search(q, types=('album',))
        
        # If no results found and multiple comma separated artists in artist text (with Tyler edge case)
        if albums.total == 0 and artist.find(', ') != -1:
            # Split into multiple artists with delimiter
            query_artist_list = artist.split(', ')

            # Loop results and build new queries
            for curr_artist in query_artist_list:
                q = get_query(curr_artist, album_name, year)
                albums, = spotify.search(q, types=('album',))
                
                # if new query turns up result, add to candidate albums
                if albums.total != 0:
                    candidate_matches += albums.items

        # If match found on initial search with pitchfork artist name, add to candidates
        else:
            candidate_matches += albums.items
        
        # Now loop through all albums and verify matches, appending verified results
        for candidate_album in candidate_matches:
            if verify_album_match(candidate_album, album_name, artist, year):
                new_match = build_album_dict(candidate_album, pitchfork_id, artist, album_name, year)
                results.append(new_match)
                album_found =  True
        
        # Modify found status
        if album_found:
            found_results += 1
        else:
            not_found_results += 1

    # Now print total found / not found
    print(f'TOTAL FOUND: {found_results} ({found_results/(found_results + not_found_results)}%)')
    print(f'TOTAL NOT FOUND {not_found_results} ({not_found_results/(found_results + not_found_results)}%)')

    ret_list.extend(results)
    # # Returns results
    # return results


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
        (chunk_0.to_dict('records'), keys.AYUSH_SPOTIFY_CLIENT_ID, keys.AYUSH_SPOTIFY_SECRET_KEY),
        (chunk_1.to_dict('records'), keys.KIMBERLY_SPOTIFY_CLIENT_ID, keys.KIMBERLY_SPOTIFY_SECRET_KEY),
        (chunk_2.to_dict('records'), keys.COLIN_SPOTIFY_CLIENT_ID, keys.COLIN_SPOTIFY_SECRET_KEY),
    ]

    # Return chunked data
    return worker_data_package


# Manager function for searching spotify for all albums
def spotify_manager(df):
    # Grab worker data in organized way
    worker_data = get_spotify_worker_data(df)
    
    # 3 keys so hard coded in
    WORKER_PROCESS_NUM = 3
    man = mp.Manager()
    ret_list = man.list()
    worker_list = []
    for i in range(WORKER_PROCESS_NUM):
        p = mp.Process(target=spotify_worker, args=(worker_data[i], ret_list))
        worker_list.append(p)
        p.start()
    
    for p in worker_list:
        p.join()
    
    worker_output = ret_list
    # Send data to the pool workers
    # with mp.Pool(WORKER_THREADS) as p:
    #     worker_output = p.starmap(spotify_worker, worker_data)

    # Now unpack everything
    # output_list = [y for x in worker_output for y in x]

    # Now convert to dataframe
    # output_df = pd.DataFrame(output_list)
    output_df = pd.DataFrame(list(worker_output))

    # Write it out
    output_df.to_csv('spotify_album_ids.csv')


if __name__ == '__main__':
    df = pd.read_csv('pitchfork_core.csv')
    df = df[['pitchfork_id', 'artist', 'album', 'release_year']]
    spotify_manager(df)