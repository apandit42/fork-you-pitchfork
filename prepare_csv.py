# Some utilities to prepare our data CSVs
import pandas as pd
import sys
import argparse

def setup_pitchfork_csv(src, dest):
    # Load the file up
    df = pd.read_csv(src)
    if 'pitchfork_id' not in df.columns:
        df.insert(0, 'pitchfork_id', range(len(df)))
    # In the working csv only have the pitchfork ID, artist name, album name, score, 
    # link to the full review, and also the release year
    df = df[['pitchfork_id', 'artist', 'album', 'score', 'link', 'release_year']]
    df.to_csv(dest, index=False)

def split_csv(src, dest_base):
    # load the file
    df = pd.read_csv(src)
    size = df.shape[0]
    n = size // 6
    for i in range(5):
        chunk_df = df.iloc[i*n:(i + 1)*n,:]
        chunk_df.to_csv(f'{dest_base}-{i}.csv', index=False)
    last_chunk_df = df.iloc[5*n:, :]
    last_chunk_df.to_csv(f'{dest_base}-{5}.csv', index=False)

# Pass in filename for file to setup
if __name__ == '__main__':
    # Will silently fail without proper columns
    parser = argparse.ArgumentParser('Prep CSVs')
    parser.add_argument('--setup', help='Setup the initial CSV', action='store_true')
    parser.add_argument('--split', help='Split the CSV into chunks (3 for now)', action='store_true')
    parser.add_argument('src', help='Source CSV')
    parser.add_argument('dest', help='Destination CSV (basename if splitting)')
    args = parser.parse_args()
    if parser.setup:
        setup_pitchfork_csv(args.src, args.dest)
    elif parser.split:
        split_csv(args.src, args.dest)
    else:
        print('No option, bink.')