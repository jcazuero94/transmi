from typing import List
import pandas as pd
from transmi.pipelines.data_science.utilities import _chgs_from_base


def system_model_fit(
    system_hourly_demand: pd.DataFrame,
    quarantines: List,
):
    return None