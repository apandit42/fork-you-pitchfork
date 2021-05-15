import pickle as pkl
import pandas as pd


with open("pitchfork_data", "rb") as f:
    object = pkl.load(f)

df = pd.DataFrame(object)
df.to_csv(r'output_pitchfork_data.csv')
