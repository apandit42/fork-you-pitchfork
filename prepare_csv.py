# Some utilities to prepare our data CSVs
import pandas as pd
import sys


def setup_pitchfork_csv(src, dest):
    # Load the file up
    df = pd.read_csv(src)
    if 'pitchfork_id' not in df.columns:
        df.insert(0, 'pitchfork_id', range(len(df)))
    # In the working csv only have the pitchfork ID, artist name, album name, score, 
    # link to the full review, and also the release year
    df = df[['pitchfork_id', 'artist', 'album', 'score', 'link', 'release_year']]
    df.to_csv(dest, index=False)

# Pass in filename for file to setup
if __name__ == '__main__':
    # Will silently fail without proper columns
    src = sys.argv[1]
    dest = sys.argv[2]
    setup_pitchfork_csv(src, dest)