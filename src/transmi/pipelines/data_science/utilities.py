import pandas as pd
from typing import List
import holidays


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
        per_chg = 365 if start_period < pd.Timestamp(2021, 4, 1) else 180
        num_chgs = int(dist_periods / per_chg)
        chgs_res += [
            start_period
            + pd.Timedelta(unit="D", value=j * int(dist_periods / (num_chgs + 1)))
            for j in range(1, num_chgs + 1)
        ]
    return chgs_res


def _get_school_recess(year: int):
    df = pd.DataFrame(
        columns=["day"],
        data=list(
            pd.date_range(
                pd.Timestamp(year=year, month=1, day=1),
                pd.Timestamp(year=year, month=12, day=31),
            )
        ),
    )
    df["weekday"] = df["day"].apply(lambda x: x.weekday())
    holidays_df = pd.DataFrame(holidays.Colombia(years=[year]).items())
    school_rec = []
    school_rec += [
        (
            pd.Timestamp(year=year, month=1, day=1),
            df.loc[
                (
                    df[df["weekday"] == 4]["day"]
                    - pd.Timestamp(year=year, month=1, day=21)
                )
                .apply(lambda x: abs(x.days))
                .sort_values()
                .index[0]
            ]["day"],
        )
    ]
    holy_thursday = pd.Timestamp(
        holidays_df[holidays_df[1] == "Jueves Santo [Maundy Thursday]"].iloc[0, 0]
    )
    school_rec += [
        (
            holy_thursday - pd.Timedelta(value=3, unit="D"),
            holy_thursday - pd.Timedelta(value=1, unit="D"),
        )
    ]
    school_rec += [
        (
            (school_rec[1][0] + pd.Timedelta(value=10 * 7, unit="D")),
            (school_rec[1][0] + pd.Timedelta(value=12 * 7 + 4, unit="D")),
        )
    ]
    school_rec += [
        (
            df.loc[
                (
                    df[df["weekday"] == 0]["day"]
                    - pd.Timestamp(year=year, month=11, day=25)
                )
                .apply(lambda x: abs(x.days))
                .sort_values()
                .index[0]
            ]["day"],
            pd.Timestamp(year=year, month=12, day=31),
        )
    ]
    return school_rec