import pandas as pd
import pickle
from pathlib import Path
import argparse
from keys import *
from search_spotify import SpotifyIdScraper


class SpotifyEndpointScraper(SpotifyIdScraper):
    def __init__(self, client_id, client_secret, filepath, writepath):
        super().__init__(client_id, client_secret, filepath, writepath)

    # Spotify API call for tracks audio features
    def get_audio_features_base(self, track_chunk):
        return self.Spotify.tracks_audio_features(track_chunk)
    
    # Spotify API call for all the tracks
    def get_tracks_base(self, track_chunk):
        return self.Spotify.tracks(track_chunk)

    # Wrapper for spotify api call with filesystem check
    def get_tracks(self, tracks_list):
        # Initial lists, note 2 endpoints are being hit
        all_tracks_tuples = []
        all_audio_features_tuples = []
        non_cached = []

        # Check cached files
        for pitchfork_id, track_id in tracks_list:
            track_filepath = Path(f'api/tracks/{pitchfork_id}_{track_id}.pickle')
            audio_features_filepath = Path(f'api/audio_features/{pitchfork_id}_{track_id}.pickle')
            if track_filepath.is_file() and audio_features_filepath.is_file():
                track_data = pickle.loads(track_filepath.read_bytes())
                audio_feature_data = pickle.loads(audio_features_filepath.read_bytes())
                all_tracks_tuples += [(pitchfork_id, track_data)]
                all_audio_features_tuples += [(pitchfork_id, audio_feature_data)]
            else:
                non_cached += [(pitchfork_id, track_id)]

        # Get the non-cached files
        CHUNK_SIZE = 50
        for i in range(0, len(non_cached), CHUNK_SIZE):
            chunk = non_cached[i:i + CHUNK_SIZE]
            chunk_pitchfork_ids, chunk_track_ids = zip(*chunk)
            track_results = self.get_tracks_base(chunk_track_ids)
            audio_feature_results = self.get_audio_features_base(chunk_track_ids)

            # now also save the newly scraped results so they will be picked up from the cache for next time
            # and also add them to the all tuples list
            for i in range(len(chunk)):
                track_filepath = Path(f'api/tracks/{chunk_pitchfork_ids[i]}_{chunk_track_ids[i]}.pickle')
                audio_features_filepath = Path(f'api/audio_features/{pitchfork_id}_{track_id}.pickle')
                track_filepath.write_bytes(pickle.dumps(track_results[i]))
                audio_features_filepath.write_bytes(pickle.dumps(audio_feature_results[i]))
                print(f'Got track {track_results[i].name} ...')
                all_tracks_tuples += [(chunk_pitchfork_ids[i], track_results[i])]
                all_audio_features_tuples += [(chunk_pitchfork_ids[i], audio_feature_results[i])]

        # Final results dicts
        track_dicts = []
        # Now we can build the rows of the tracks csv
        for i in range(len(all_tracks_tuples)):
            pitchfork_id, track_data = all_tracks_tuples[i]
            pitchfork_id, audio_features_data = all_audio_features_tuples[i]
            track_dicts += [{
                'pitchfork_id': pitchfork_id,
                'track_id': track_data.id,
                'artist_id': '|'.join([x.id for x in track_data.artists]),
                'duration': track_data.duration_ms,
                'explicit': track_data.explicit,
                'name': track_data.name,
                'track_number': track_data.track_number,
                'popularity': track_data.popularity,
                'acousticness': audio_features_data.acousticness,
                'danceability': audio_features_data.danceability,
                'energy': audio_features_data.energy,
                'instrumentalness': audio_features_data.instrumentalness,
                'key': audio_features_data.key,
                'liveness': audio_features_data.liveness,
                'loudness': audio_features_data.loudness,
                'mode': audio_features_data.mode,
                'speechiness': audio_features_data.speechiness,
                'tempo': audio_features_data.tempo,
                'time_signature': audio_features_data.time_signature,
                'valence': audio_features_data.valence,
            }]

        return track_dicts

    # Scrape tracks
    def scrape_tracks(self):
        # First get the right columns of dataframe
        tracks_df = self.df[['pitchfork_id', 'track_ids']]
        raw_tracks_tuples = list(tracks_df.to_records(index=False))

        # Now make sure that each track id gets its own row in the tuples
        full_tracks_tuples = []
        for pitchfork_id, track_ids in raw_tracks_tuples:
            full_tracks_tuples += [(pitchfork_id, split_id) for split_id in track_ids.split('|')]

        # Now pass this list into get_artists, expect ready to write list of dicts back
        tracks_data_dicts = self.get_tracks(full_tracks_tuples)

        # Save the output
        output_pickle = Path(f'{self.writepath}.pickle')
        output_pickle.write_bytes(pickle.dumps(tracks_data_dicts))
        final_tracks_df = pd.DataFrame(tracks_data_dicts)
        final_tracks_df.to_csv(self.writepath, index=False)
    
    # Scrape all the tracks
    def scrape_tracks(self):
        # Get right columns from df
        tracks_df = self.df[['pitchfork_id', '']]

    # Spotify API call for all the artists
    def get_artists_base(self, artist_chunk):
        return self.Spotify.artists(artist_chunk)
    
    # Wrapper for spotify API call that checks filesystem
    def get_artists(self, artist_list):
        # Initial lists
        all_tuples = []
        non_cached = []

        # Check which files have been cached already
        # if artists come up over again, just pull them once
        for pitchfork_id, artist_id in artist_list:
            filepath = Path(f'api/artists/{artist_id}.pickle')
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
                filepath = Path(f'api/artists/{chunk_artist_ids[i]}.pickle')
                filepath.write_bytes(pickle.dumps(artist_results[i]))
                print(f'Got artist {artist_results[i].name} ...')
                all_tuples += [(chunk_pitchfork_ids[i], artist_results[i])]
        
        # Final results dicts list
        artist_dicts = []
        # we can evaluate the artist data and build dict list for artists csv
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
            full_artist_tuples += [(pitchfork_id, split_id) for split_id in artist_id.split('|')]
        
        # Now pass this list to get_artists, expect a list of dicts back
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
                'artist_id': '|'.join([x.id for x in best_match_album.artists]),
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
    parser.add_argument('--tracks', help='Find the track information.', action='store_true')
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
    if args.tracks:
        chungus.scrape_tracks()
