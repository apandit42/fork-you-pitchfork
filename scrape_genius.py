import pandas as pd
import pickle
from pathlib import Path
import lyricsgenius as lg
import argparse
from keys import *


class GeniusScraper:
    def __init__(self, client_token, src_file, dest_file):
        self.client_token = client_token
        self.Genius = lg.Genius(client_token)
        self.src_file = src_file
        self.df = pd.read_csv(src_file)
        self.dest_file = dest_file
    
    # Pull all of the data


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Pull from Genius')
    # By default, use Ayush's Genius Token
    parser.add_argument('src', help='Source CSV')
    parser.add_argument('dest', help='Destination CSV')
    args = parser.parse_args()
    geniusGoon = GeniusScraper(AYUSH_GENIUS_TOKEN, args.src, args.dest)
    
    