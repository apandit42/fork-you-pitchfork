import pandas as pd
import pickle
from pathlib import Path
import argparse
from keys import *
from search_spotify import SpotifyIdScraper


class SpotifyEndpointScraper(SpotifyIdScraper):
    def __init__(self, client_id, client_secret, filepath, writepath):
        super().__init__(client_id, client_secret, filepath, writepath)
    
    # Spotify API call for all the tracks
    def get_tracks_base(self, track_chunk):
        pass

    # Wrapper for spotify api call with filesystem check
    def get_tracks(self, track_chunk):
        pass
    
    # Spotify API call for all the artists
    def get_artists_base(self, artist_chunk):
        return self.Spotify.artists(artist_chunk)
    
    # Wrapper for spotify API call that checks filesystem
    def get_artists(self, artist_list):
        # Initial lists
        all_tuples = []
        non_cached = []

        # Check which files have been cached already
        for pitchfork_id, artist_id in artist_list:
            filepath = Path(f'api/artists/{pitchfork_id}_{artist_list}.pickle')
            if filepath.is_file():
                data = pickle.loads(filepath.read_bytes())
                all_tuples += [(pitchfork_id, data)]
            else:
                non_cached += [(pitchfork_id, artist_id)]
        
        # Chunk the non-cached files and get their album data
        CHUNK_SIZE = 50
        for i in range(0, len(non_cached), CHUNK_SIZE):
            chunk = non_cached[i:i + CHUNK_SIZE]
            chunk_pitchfork_ids, chunk_artist_ids = zip(*chunk)
            artist_results = self.get_artists_base(chunk_artist_ids)

            # now save these new results to files so they wont be pulled again, while also adding them to
            # the list of all match tuples
            for i in range(len(chunk)):
                filepath = Path(f'api/artists/{chunk_pitchfork_ids[i]}_{chunk_artist_ids[i]}.pickle')
                filepath.write_bytes(pickle.dumps(artist_results[i]))
                print(f'Got artist {artist_results[i].name} ...')
                all_tuples += [(chunk_pitchfork_ids[i], artist_results[i])]
        
        # Final results dicts list
        artist_dicts = []
        # Now that we have all of the grouped matches, we can evaluate the best album and build dict list
        for pitchfork_id, artist in all_tuples:
            artist_dicts += [{
                'pitchfork_id': pitchfork_id,
                'artist_name': artist.name,
                'popularity': artist.popularity,
                'artist_id': artist.id,
                'followers': artist.followers.total,
                'genres': '|'.join([x for x in artist.genres])
            }]
        
        return artist_dicts
    
    # Scrape artists
    def scrape_artists(self):
        # Get df with the right columns
        artists_df = self.df[['pitchfork_id', 'artist_id']]
        raw_artist_tuples = list(artists_df.to_records(index=False))

        # Fix the multiple artists situation, now each artist will have their own row, but still
        # be connected to the original pitchfork_id row from the first dataset
        full_artist_tuples = []
        for pitchfork_id, artist_id in raw_artist_tuples:
            full_artist_tuples += [(pitchfork_id, split_id) for split_id in artist_id.split(',')]
        
        # Now pass this list to get_artists, expect a dict back
        artist_data_dicts = self.get_artists(full_artist_tuples)

        # Save the output files
        output_pickle = Path(f'{self.writepath}.pickle')
        output_pickle.write_bytes(pickle.dumps(artist_data_dicts))
        final_artists_df = pd.DataFrame(artist_data_dicts)
        final_artists_df.to_csv(self.writepath, index=False)

    # Spotify API call for all the albums
    def get_albums_base(self, album_chunk):
        return self.Spotify.albums(album_chunk)

    # Gets the "best" matching album of a list of albums,
    # based on the most popular album and having an explicit version
    def get_best_album_match_from_list(self, album_list):
        # List of filtered candidates
        best_candidates = []

        # First, try to only get explicit albums
        for album in album_list:
            song_explicit_ratings = [track.explicit for track in album.tracks.items]
            if any(song_explicit_ratings):
                best_candidates += [album]
        # If there are no explicit albums though, just add the whole list back
        if len(best_candidates) == 0:
            best_candidates = album_list
        
        # Now, just get the album with max popularity
        popularity_and_album = [(album.popularity, album) for album in best_candidates]
        highest_popularity, most_popular_album = max(popularity_and_album, key=lambda x:x[0])
        print(f'Most popular album is {most_popular_album.name} w/ score {highest_popularity} ...')
        return most_popular_album

    # Wrapper for spotify API call that checks filesystem
    def get_albums(self, albums_list):
        # Initial lists
        all_matches_tuples = []
        non_cached_albums = []

        # Check which files have been cached already
        for pitchfork_id, album_id in albums_list:
            filepath = Path(f'api/albums/{pitchfork_id}_{album_id}.pickle')
            if filepath.is_file():
                data = pickle.loads(filepath.read_bytes())
                all_matches_tuples += [(pitchfork_id, data)]
            else:
                non_cached_albums += [(pitchfork_id, album_id)]
        
        # Chunk the non-cached files and get their album data
        CHUNK_SIZE = 20
        for i in range(0, len(non_cached_albums), CHUNK_SIZE):
            chunk = non_cached_albums[i:i + CHUNK_SIZE]
            chunk_pitchfork_ids, chunk_album_ids = zip(*chunk)
            albums_results = self.get_albums_base(chunk_album_ids)

            # now save these new results to files so they wont be pulled again, while also adding them to
            # the list of all match tuples
            for i in range(len(chunk)):
                filepath = Path(f'api/albums/{chunk_pitchfork_ids[i]}_{chunk_album_ids[i]}.pickle')
                filepath.write_bytes(pickle.dumps(albums_results[i]))
                all_matches_tuples += [(chunk_pitchfork_ids[i], albums_results[i])]
        
        # Now go through all the tuples in all_matches_tuples, and construct a dict
        grouped_matches = {}
        for pitchfork_id, data in all_matches_tuples:
            if pitchfork_id in grouped_matches:
                grouped_matches[pitchfork_id] += [data]
            else:
                grouped_matches[pitchfork_id] = [data]
        
        # Final results dicts list
        best_matches_dicts = []
        # Now that we have all of the grouped matches, we can evaluate the best album and build dict list
        for pitchfork_id in grouped_matches:
            best_match_album = self.get_best_album_match_from_list(grouped_matches[pitchfork_id])
            best_matches_dicts += [{
                'pitchfork_id': pitchfork_id,
                'album_name': best_match_album.name,
                'album_id': best_match_album.id,
                'label': best_match_album.label,
                'popularity': best_match_album.popularity,
                'release_date': best_match_album.release_date,
                'release_date_precision': best_match_album.release_date_precision,
                'total_tracks': best_match_album.total_tracks,
                'track_ids': '|'.join([x.id for x in best_match_album.tracks.items])
            }]
        
        return best_matches_dicts

    # Scrape best matching albums per pitchfork id
    def scrape_best_album_matches(self):
        # Conver to tuples
        matches_df = self.df[['pitchfork_id', 'album_id']]
        matches_tuples = list(matches_df.to_records(index=False))

        # Get best matches as list of dicts
        best_matches_dicts = self.get_albums(matches_tuples)
        
        # Save the files and write them out
        output_pickle = Path(f'{self.writepath}.pickle')
        output_pickle.write_bytes(pickle.dumps(best_matches_dicts))
        best_matches_df = pd.DataFrame(best_matches_dicts)
        best_matches_df.to_csv(self.writepath, index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Search Spotify')
    parser.add_argument('--ap', help='Ayush key', action='store_true')
    parser.add_argument('--ck', help='Colin key', action='store_true')
    parser.add_argument('--kb', help='Kimberly key', action='store_true')
    parser.add_argument('--albums', help='Find best album matches.', action='store_true')
    parser.add_argument('--artists', help='Find the artist information.', action='store_true')
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
    chungus = SpotifyEndpointScraper(client_id, client_secret, args.src, args.dest)
    if args.albums:
        chungus.scrape_best_album_matches()
    if args.artists:
        chungus.scrape_artists()
