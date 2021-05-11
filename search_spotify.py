import multiprocessing as mp
import pandas as pd
from pprint import pprint
import tekore as tk
from unidecode import unidecode
import argparse
import pickle
from pathlib import Path
import hashlib
from keys import *
import re

class SpotifyIdScraper:
    def __init__(self, client_id, client_secret, filepath, writepath):
        self.app_token = tk.request_client_token(client_id, client_secret)
        self.Spotify = tk.Spotify(self.app_token, sender=tk.RetryingSender(retries=1000))
        self.filepath = filepath
        self.df = pd.read_csv(filepath)
        self.writepath = writepath
        self.core_df = self.df[['pitchfork_id', 'artist', 'album']]

    # Build the query string for spotify search calls
    def get_query(self, artist, album):
        q = f'album:{album} artist:{artist}'
        return q 

    # Clean all text and prep for searches
    def clean_text(self, text):
        if pd.isna(text):
            text = ''
        elif isinstance(text, float):
            text = int(text)
        text = str(text)
        text = unidecode(text).strip().lower()
        return text
    
    # Build each album dict for the list
    def build_album_dict(self, album, pitchfork_id, artist, album_name):
        artist_id = ','.join([x.id for x in album.artists])
        album_info_dict = {
            'pitchfork_id': pitchfork_id,
            'album': album_name,
            'album_id': album.id,
            'artist': artist,
            'artist_id': artist_id,
        }
        return album_info_dict

    # Verify a list of albums
    def verify_album_list(self, album_list, wanted_album, wanted_artist):
        verified_list = []
        for album in album_list:
            if self.verify_album_match(album, wanted_album, wanted_artist):
                verified_list += [album]
        return verified_list

    # Verify that the albums match the pitchfork album name, artist, and year
    def verify_album_match(self, album, wanted_album, wanted_artist):
        # If it's just 1 track, not a match
        if album.total_tracks == 1:
            return False

        # If the names don't match, not the right thing
        candidate_name = self.clean_text(album.name)
        if wanted_album != candidate_name:
            print(f'Album names didnt match {wanted_album} vs {candidate_name}')
            return False

        # Check if artists match, first single artist case, then multi artist
        artists_found = album.artists
        if len(artists_found) == 1:
            artist_name = self.clean_text(artists_found[0].name)
            if artist_name not in wanted_artist and wanted_artist not in artist_name:
                print(f'Artists dont match {artists_found[0].name} vs {wanted_artist}')
                return False
            else:
                return True
        else:
            # NOTE: Might change with new JSON
            wanted_artist_set = {self.clean_text(x) for x in wanted_artist.split('||')}
            artists_found_set = {self.clean_text(y) for x in artists_found for y in x.name.split('||')}
            if len(wanted_artist_set & artists_found_set) == 0:
                print(f'MULTI artists dont match {artists_found[0].name} vs {wanted_artist}')
                return False
            else:
                return True

    # get hash hex digest for string data
    def get_hash(self, data):
        return hashlib.md5(data.encode()).hexdigest()
    
    # search wrapper with file loading checks
    def search(self, artist, album_name):
        q = self.get_query(artist, album_name)
        cache_str = self.get_hash(q)
        cache_file = Path(f'api/search/{cache_str}.pickle')
        if cache_file.is_file():
            cache_data = pickle.loads(cache_file.read_bytes())
            return cache_data
        else:
            data = self.base_search(artist, album_name)
            cache_file.write_bytes(pickle.dumps(data))
            return data

    # base search to get albums
    def base_search(self, artist, album_name):
        q = self.get_query(artist, album_name)
        albums, = self.Spotify.search(q, types=('album',))
        return self.verify_album_list(albums.items, album_name, artist)
    
    # remove ep from pitchfork names (if there)
    def remove_ep(self, album_name):
        if album_name.find(' ep ') != -1 or album_name.endswith(' ep'):
            album_name = album_name.replace(' ep ', '').replace(' ep', '')
        return album_name

    # replace and with &
    def replace_and(self, name):
        if name.find(' and ') != -1:
            name = name.replace(' and ', ' & ')
        return name
    
    # replace & with and
    def replace_and_reverse(self, name):
        if name.find(' & ') != -1:
            name = name.replace(' and ', ' & ')
        return name
    
    # add remastered
    def replace_remastered(self, album_name):
        return album_name + ' (remastered)'

    # scrapes and writes it
    def scrape_and_store(self):
        results = self.scrape()
        dumpfile = open(f'{self.writepath}.pickle', mode='wb')
        pickle.dump(results, dumpfile)
        dumpfile.close()
        results_df = pd.DataFrame(results)
        results_df.to_csv(self.writepath, index=False)
    
    # Scrape
    def scrape(self):
        # begin results
        results = []

        # Counters
        found = 0
        not_found = 0
        counter = 0

        # now loop df
        for pitchfork_id, artist, album_name in self.core_df.itertuples(index=False, name=None):
            # Clean variables, set flag, & print confirmation
            artist = self.clean_text(artist)
            album_name = self.clean_text(album_name)
            candidate_matches = []
            print(f'ID {pitchfork_id} w/ {album_name} by {artist} ...')
            
            if artist.find('||'):
                artist_list = [artist] + [self.clean_text(x) for x in artist.split('||')]
            else:
                artist_list = [artist]

            # loop through each
            for curr_artist in artist_list:
                if len(candidate_matches) != 0:
                    break
                
                candidate_matches += self.search(curr_artist, album_name)
                
                # removing EPs
                if len(candidate_matches) == 0:
                    updated_album = self.remove_ep(album_name)
                    candidate_matches += self.search(curr_artist, updated_album)
                
                # removing and w/ & album
                if len(candidate_matches) == 0:
                    updated_album = self.replace_and(album_name)
                    candidate_matches += self.search(curr_artist, updated_album)
                
                # removing and w/ & artist
                if len(candidate_matches) == 0:
                    updated_artist = self.replace_and(artist)
                    candidate_matches += self.search(updated_artist, album_name)
                
                # adding remastered
                if len(candidate_matches) == 0:
                    updated_album = self.replace_remastered(album_name)
                    candidate_matches += self.search(artist, updated_album)
                
                # Now in combination, ep and and
                if len(candidate_matches) == 0:
                    updated_album = self.remove_ep(album_name)
                    updated_album = self.replace_and(album_name)
                    candidate_matches += self.search(artist, updated_album)
                
                # Now in combination, ep and reverse and
                if len(candidate_matches) == 0:
                    updated_album = self.remove_ep(album_name)
                    updated_album = self.replace_and_reverse(album_name)
                    candidate_matches += self.search(artist, updated_album)
                
                # ep and and and remastered
                if len(candidate_matches) == 0:
                    updated_album = self.remove_ep(album_name)
                    updated_album = self.replace_and(album_name)
                    updated_album = self.replace_remastered(album_name)
                    candidate_matches += self.search(artist, updated_album)

                # Now in combination, ep and reverse and and remastered
                if len(candidate_matches) == 0:
                    updated_album = self.remove_ep(album_name)
                    updated_album = self.replace_and_reverse(album_name)
                    updated_album = self.replace_remastered(album_name)
                    candidate_matches += self.search(artist, updated_album)
                
                # Now all above + the name and stuff (and and)
                if len(candidate_matches) == 0:
                    updated_album = self.remove_ep(album_name)
                    updated_album = self.replace_and(album_name)
                    updated_album = self.replace_remastered(album_name)
                    updated_artist = self.replace_and(artist)
                    candidate_matches += self.search(updated_artist, updated_album)
                
                # Now all above + the name reverse and stuff
                if len(candidate_matches) == 0:
                    updated_album = self.remove_ep(album_name)
                    updated_album = self.replace_and_reverse(album_name)
                    updated_album = self.replace_remastered(album_name)
                    updated_artist = self.replace_and_reverse(artist)
                    candidate_matches += self.search(updated_artist, updated_album)
                
                # Now remove parentheses + ep
                if len(candidate_matches) == 0:
                    updated_album = self.remove_ep(album_name)
                    updated_album = re.sub(r'\s*\(.*\)\s*', r'', album_name)
                    candidate_matches += self.search(updated_artist, updated_album)
                
                # Now remove parentheses + other tings
                if len(candidate_matches) == 0:
                    updated_album = re.sub(r'\s*\(.*\)\s*', r'', album_name)
                    updated_album = self.remove_ep(album_name)
                    updated_album = self.replace_and(album_name)
                    candidate_matches += self.search(updated_artist, updated_album)
            
            # Back out of the loop
            
            # Now loop through all albums and append verified results
            for candidate_album in candidate_matches:
                print(f'{candidate_album.name} by {candidate_album.artists[0].name}')
                new_match = self.build_album_dict(candidate_album, pitchfork_id, artist, album_name)
                results += [new_match]
            
            # Modify found status
            if len(candidate_matches):
                found += 1
            else:
                print(f'\n*********************\nALBUM NOT FOUND: {album_name} by {artist} !\n**************************\n')
                not_found += 1
            counter += 1

        # Now print total found / not found
        print(f'TOTAL FOUND: {found} ({100 * found/(found + not_found)}%)')
        print(f'TOTAL NOT FOUND {not_found} ({100 * not_found/(found + not_found)}%)')
        return results
        

if __name__ == '__main__':
    parser = argparse.ArgumentParser('Search Spotify')
    parser.add_argument('--ap', help='Ayush key', action='store_true')
    parser.add_argument('--ck', help='Colin key', action='store_true')
    parser.add_argument('--kb', help='Kimberly key', action='store_true')
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
    scraperGoon = SpotifyIdScraper(client_id, client_secret, args.src, args.dest)
    scraperGoon.scrape_and_store()