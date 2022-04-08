import pandas as pd
from typing import List


def _chgs_from_base(chgs_base: List, start_chgs, end_chgs):
    chgs_base.sort()
    chgs_base_comp = [start_chgs] + chgs_base + [end_chgs]
    chgs_res = []
    # Strong trend change due to restrictions
    for ch in chgs_base:
        chgs_res += [
            ch,
            ch + pd.Timedelta(value=1, unit="days"),
            ch - pd.Timedelta(value=1, unit="days"),
            ch + pd.Timedelta(value=3, unit="days"),
            ch - pd.Timedelta(value=3, unit="days"),
            ch + pd.Timedelta(value=5, unit="days"),
            ch - pd.Timedelta(value=5, unit="days"),
        ]
    # Normal trend evolution
    for i in range(len(chgs_base_comp) - 1):
        start_period = chgs_base_comp[i]
        end_period = chgs_base_comp[i + 1]
        dist_periods = (end_period - start_period).days
        num_chgs = int(dist_periods / 365)
        chgs_res += [
            start_period
            + pd.Timedelta(unit="D", value=j * int(dist_periods / (num_chgs + 1)))
            for j in range(1, num_chgs + 1)
        ]
    return chgs_res