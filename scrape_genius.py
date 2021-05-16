import pandas as pd
import time
import random
import pickle
from pathlib import Path
import lyricsgenius as lg
import argparse
from unidecode import unidecode
from multiprocessing import Pool
from keys import *


class GeniusScraper:
    def __init__(self, client_token, src_file, dest_file):
        self.client_token = client_token
        self.Genius = lg.Genius(client_token, retries=10)
        self.src_file = src_file
        self.df = pd.read_csv(src_file)
        self.dest_file = dest_file
    
    # replace & with and
    def replace_and_reverse(self, name):
        if name.find(' & ') != -1:
            name = name.replace(' and ', ' & ')
        return name
    
    # Worker thread
    def worker_thread(self, row):
        time.sleep(random.randint(0, 5))
        # Generate a new genius
        workerGenius = lg.Genius(self.client_token, retries=10)
        # Unpack row
        pitchfork_id, track_id, name, artist_name = row
        # Check cache
        track_file = Path(f'api/genius_tracks/{pitchfork_id}_{track_id}.pickle')
        if track_file.is_file():
            genius_data = pickle.loads(track_file.read_bytes())
        else:
            split_artists = artist_name.split('|')
            genius_data = None
            for artist in split_artists:
                song = workerGenius.search_song(title=name, artist=artist)
                if song is not None and song.title == name and song.artist == artist:
                    genius_data = song
            if genius_data is None:
                for artist in split_artists:
                    q_name = self.replace_and_reverse(unidecode(name).lower().strip())
                    q_artist = self.replace_and_reverse(unidecode(artist).lower().strip())
                    song = workerGenius.search_song(title=q_name, artist=q_artist)
                    if song is not None and song.title == q_name and song.artist == q_artist:
                        genius_data = song
        # Now we have a genius data var populated, just return the written dict
        if genius_data is None:
            return None
        stats_data = genius_data.stats.__dict__
        genius_dict = {
            'pitchfork_id': pitchfork_id,
            'track_id': track_id,
            'name': name,
            'artist_name': artist_name,
            'genius_name': genius_data.title,
            'genius_full_name': genius_data.full_title,
            'genius_id': genius_data.id,
            'lyrics': genius_data.lyrics,
            'genius_url': genius_data.url,
            'genius_hot': stats_data.get('hot', ''),
            'genius_pageviews': stats_data.get('pageviews', '')
        }
        return genius_dict

    # multi thread
    def main_multi_thread(self):
        core_df = self.df[['pitchfork_id', 'track_id', 'name', 'artist_name']]
        raw_tracks = list(core_df.to_records(index=False))

        MAX_THREAD = 64
        with Pool(MAX_THREAD) as p:
            genius_data_dicts = p.map(self.worker_thread, raw_tracks)
        
        # Write it out
        outpickle = Path(f'{self.dest_file}.pickle')
        outpickle.write_bytes(pickle.dumps(genius_data_dicts))
        genius_df = pd.DataFrame(genius_data_dicts)
        genius_df.to_csv(self.dest_file)
    
    # Get all the genius data
    def get_all_genius_data(self, raw_tracks):
        all_tracks = []
        non_cached = []

        # Check the cache
        for pitchfork_id, track_id, name, artist_name in raw_tracks:
            genius_track_file = Path(f'api/genius_tracks/{pitchfork_id}_{track_id}.pickle')
            if genius_track_file.is_file():
                genius_track_data = pickle.loads(genius_track_file.read_bytes())
                all_tracks += [(pitchfork_id, track_id, name, artist_name, genius_track_data)]
            else:
                non_cached += [(pitchfork_id, track_id, name, artist_name)]
        
        # Get the non cached bois
        for pitchfork_id, track_id, name, artist_name in non_cached:
            # Split into mutliple artist names
            artist_names = artist_name.split('|')
            # Best candidate
            candidate = None

            # Try to grab the song on genius, first easy pass
            # we're just gonna try catch this chief
            try:
                for split_name in artist_names:
                    song = self.Genius.search_song(title=name, artist=split_name)
                    if song is not None and song.title == name and song.artist == split_name:
                        candidate = song
                # If there's no match, keep looking
                if candidate is None:
                    for split_name in artist_names:
                        song = self.Genius.search_song(title=name.lower(), artist=split_name.lower())
                        if song is not None and song.title == name and song.artist == split_name:
                            candidate = song
                # Still no match? Still keep looking
                if candidate is None:
                    for split_name in artist_names:
                        q_name = self.replace_and_reverse(unidecode(name).lower().strip())
                        q_artist = self.replace_and_reverse(unidecode(split_name).lower().strip())
                        song = self.Genius.search_song(title=q_name, artist=q_artist)
                        if song is not None and song.title == q_name and song.artist == q_artist:
                            candidate = song
            except Exception:
                print(f'EXCEPTION EXCEPTION EXCEPTION')
                print(f'Skipping {pitchfork_id}, {name}, {artist_name} for now...')
                continue
                
            # At this point, just write the candidate out, fuck it
            genius_track_file = Path(f'api/genius_tracks/{pitchfork_id}_{track_id}.pickle')
            genius_track_file.write_bytes(pickle.dumps(candidate))
            
            # Now add it to the found all tracks
            all_tracks += [(pitchfork_id, track_id, name, artist_name, candidate)]
        
        # Now go through all the goons, and return the genius data dicts
        found = 0
        not_found = 0
        genius_dicts = []
        for pitchfork_id, track_id, name, artist_name, genius_data in all_tracks:
            if genius_data is None:
                not_found += 1
                continue
            print(f'Found {name} by {artist_name} (Pitchfork {pitchfork_id})...')
            stats_data = genius_data.stats.__dict__
            genius_dicts += [{
                'pitchfork_id': pitchfork_id,
                'track_id': track_id,
                'name': name,
                'artist_name': artist_name,
                'genius_name': genius_data.title,
                'genius_full_name': genius_data.full_title,
                'genius_id': genius_data.id,
                'lyrics': genius_data.lyrics,
                'genius_url': genius_data.url,
                'genius_hot': stats_data.get('hot', ''),
                'genius_pageviews': stats_data.get('pageviews', '')
            }]
            found += 1
        print(f'\n\n\n***********************FOUND TOTAL: {found}\nNOT FOUND: {not_found}\nRATIO: {found / not_found}*****************************\n\n\n')
        return genius_dicts
    
    # Pull all of the data
    def scrape_genius(self):
        core_df = self.df[['pitchfork_id', 'track_id', 'name', 'artist_name']]
        raw_tracks = list(core_df.to_records(index=False))

        # Get genius data dicts
        genius_data_dicts = self.get_all_genius_data(raw_tracks)
        # Write it out
        outpickle = Path(f'{self.dest_file}.pickle')
        outpickle.write_bytes(pickle.dumps(genius_data_dicts))
        genius_df = pd.DataFrame(genius_data_dicts)
        genius_df.to_csv(self.dest_file)


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Pull from Genius')
    # By default, use Ayush's Genius Token
    # Designed to work with the MATCHED_TRACK data set
    parser.add_argument('src', help='Source CSV')
    parser.add_argument('dest', help='Destination CSV')
    args = parser.parse_args()
    geniusGoon = GeniusScraper(AYUSH_GENIUS_TOKEN, args.src, args.dest)
    geniusGoon.scrape_genius()
    
