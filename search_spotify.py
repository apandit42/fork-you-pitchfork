import multiprocessing as mp
import pandas as pd
from pprint import pprint
import tekore as tk


# Build the query string for spotify search calls
def get_query(artist, album):
    q = f'album:{album} artist:{artist}'
    return q


# Clean all text and prep for searches
def clean_text(text):
    if pd.isna(text):
        text = ''
    elif isinstance(text, float):
        text = int(text)
    text = str(text)
    text = text.strip().lower().replace('“','"').replace('”','"')
    text = text.replace('’',"'").replace('','')
    return text


# Verify a list of albums
def verify_album_list(album_list, wanted_album, wanted_artist):
    verified_list = []
    for album in album_list:
        if verify_album_match(album, wanted_album, wanted_artist):
            verified_list += [album]
    # retry by comparing wanted album w remastered editions
    if len(verified_list) == 0:
        wanted_album += ' (remastered)'
        for album in album_list:
            if verify_album_match(album, wanted_album, wanted_artist):
                verified_list += [album]
    return verified_list


# Verify that the albums match the pitchfork album name, artist, and year
def verify_album_match(album, wanted_album, wanted_artist):
    candidate_name = clean_text(album.name)
    if wanted_album != candidate_name:
        print(f'Album names didnt match {wanted_album} vs {candidate_name}')
        return False

    # Check if artists match, first single artist case, then multi artist
    artists_found = album.artists
    if len(artists_found) == 1:
        artist_name = clean_text(artists_found[0].name)
        if artist_name not in wanted_artist and wanted_artist not in artist_name:
            print(f'Artists dont match {artists_found[0].name} vs {wanted_artist}')
            return False
        else:
            return True
    else:
        wanted_artist_set = {clean_text(x) for x in wanted_artist.split(', ')}
        artists_found_set = {clean_text(y) for x in artists_found for y in x.name.split(', ')}
        if len(wanted_artist_set.intersection(artists_found_set)) == 0:
            print(f'MULTI artists dont match {artists_found[0].name} vs {wanted_artist}')
            return False
        else:
            return True


# Build each album dict for the list
def build_album_dict(album, pitchfork_id, artist, album_name):
    artist_id = ','.join([x.id for x in album.artists])
    album_info_dict = {
        'pitchfork_id': pitchfork_id,
        'album': album_name,
        'album_id': album.id,
        'artist': artist,
        'artist_id': artist_id,
    }
    return album_info_dict


# Spotify worker threads
def spotify_scraper(df):
    # Setup tk Spotify session
    app_token  = tk.request_client_token('0fd39efec46f48228625be76ef6981e7', '38a623ad82d44e8d9c1f5b712e6486e8')
    spotify = tk.Spotify(app_token, sender=tk.RetryingSender(retries=1000))

    # Begin the results list which will hold all the row dicts
    results = []

    # Counter of found results + not found results
    found_results = 0
    not_found_results = 0
    counter = 0

    # loop through each row in pitchfork data to find matching album on spotify 
    for pitchfork_id, artist, album_name in df.itertuples(index=False, name=None):
        # Clean variables, set flag, & print confirmation
        artist = clean_text(artist)
        album_name = clean_text(album_name)
        candidate_matches = []
        print(f'ID {pitchfork_id} w/ {album_name} by {artist} ...')

        # Build search query & get initial search results
        q = get_query(artist, album_name)
        albums, = spotify.search(q, types=('album',))
        candidate_matches += verify_album_list(albums.items, album_name, artist)
        
        # First try if its an ep, removing ep
        if len(candidate_matches) == 0 and album_name.endswith(' ep', -3):
            q = get_query(artist, album_name[:-3])
            albums, = spotify.search(q, types=('album',))
            candidate_matches += verify_album_list(albums.items, album_name[:-3], artist)

        # Try to replace 'and' with '&' 
        if len(candidate_matches) == 0 and (' and ' in album_name or ' and ' in artist):
            updated_artist_name = artist.replace(' and ', ' & ')
            updated_album_name = album_name.replace(' and ', ' & ')
            q = get_query(updated_artist_name, updated_album_name)
            albums, = spotify.search(q, types=('album',))
            candidate_matches += verify_album_list(albums.items, updated_album_name, updated_artist_name)
        
        if len(candidate_matches) == 0 and (' and ' in album_name or ' and ' in artist) and album_name.endswith(' ep', -3):
            updated_album_name = album_name.replace(' and ', ' & ')
            updated_artist_name = artist.replace(' and ', ' & ')
            q = get_query(updated_artist_name, updated_album_name[:-3])
            albums, = spotify.search(q, types=('album',))
            candidate_matches += verify_album_list(albums.items, updated_album_name, updated_artist_name)
        
        # If that's not enough, try splitting up the artist
        if len(candidate_matches) == 0 and artist.find(', ') != -1:
            # Split into multiple artists with delimiter
            query_artist_list = [clean_text(x) for x in artist.split(', ')]

            # Loop results and build new queries
            for curr_artist in query_artist_list:
                q = get_query(curr_artist, album_name)
                albums, = spotify.search(q, types=('album',))
                candidate_matches += verify_album_list(albums.items, album_name, artist)
        
        # Now loop through all albums and verify matches, appending verified results
        for candidate_album in candidate_matches:
            print(f'{candidate_album.name} by {candidate_album.artists[0].name}')
            new_match = build_album_dict(candidate_album, pitchfork_id, artist, album_name)
            results.append(new_match)
        
        # Modify found status
        if len(candidate_matches):
            found_results += 1
        else:
            print(f'\n*********************\nALBUM NOT FOUND: {album_name} by {artist}!\n**************************\n')
            not_found_results += 1

    # Now print total found / not found
    print(f'TOTAL FOUND: {found_results} ({100 * found_results/(found_results + not_found_results)}%)')
    print(f'TOTAL NOT FOUND {not_found_results} ({100 * not_found_results/(found_results + not_found_results)}%)')
    return results


if __name__ == '__main__':
    df = pd.read_csv('pitchfork_core.csv')
    df = df[['pitchfork_id', 'artist', 'album']]
    result_list = spotify_scraper(df)
    output_df = pd.DataFrame(result_list)
    output_df.to_csv('spotify_album_and_artist_ids.csv', index=False)