import xgboost
import sklearn
import pandas as pd
import pickle
from pathlib import Path

__version__="1.0"

BASE_DIR = Path(__file__).resolve(strict=True).parent

with open(f'{BASE_DIR}/modelstocks_{__version__}.pkl', "rb") as f:
    model = pickle.load(f)

def predict_volume(vol_moving_avg, adj_close_rolling_med):

    input = pd.DataFrame({
    'vol_moving_avg': [vol_moving_avg],
    'adj_close_rolling_med': [adj_close_rolling_med]})

    return model.predict(input)[0]
