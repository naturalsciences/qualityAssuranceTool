import numpy as np
import pandas as pd


def min_max_check_values(values: pd.DataFrame, min_: float, max_: float):
    out = np.logical_and(values >= min_, values <= max_)
    return out
