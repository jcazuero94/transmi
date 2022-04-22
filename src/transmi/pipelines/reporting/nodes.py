import pandas as pd


def prepare_hourly_system_model_app(
    forecast_system: pd.DataFrame, holidays_df: pd.DataFrame
):
    """Prepare forecast_system for app vsualization"""
    # Dayly seasonalities
    dayly_seasonalities = ["weekday", "schoolday", "saturday", "sunday", "holiday"]
    dayly_seasonalities_df = None
    for col in dayly_seasonalities:
        aux_df = forecast_system[["ds", col]].iloc[: 180 * 24].copy()
        aux_df["hour"] = aux_df["ds"].apply(lambda x: x.hour)
        aux_df.drop("ds", axis=1, inplace=True)
        aux_df = aux_df[aux_df[col] != 0].copy()
        aux_df[col] = aux_df[col].apply(lambda x: round(x, 5))
        aux_df.drop_duplicates(inplace=True)
        aux_df.rename({col: "value"}, axis=1, inplace=True)
        aux_df["day"] = col
        dayly_seasonalities_df = pd.concat(
            [dayly_seasonalities_df, aux_df], ignore_index=True
        )

    # Weekly seasonality
    weekly_seasonality = forecast_system.iloc[: 24 * 14][["ds", "weekly"]].copy()
    weekly_seasonality["day"] = weekly_seasonality["ds"].apply(lambda x: x.weekday())
    weekly_seasonality["hour"] = weekly_seasonality["ds"].apply(lambda x: x.hour)
    weekly_seasonality.drop_duplicates(["day", "hour"], inplace=True)
    weekly_seasonality = (
        weekly_seasonality.sort_values(["day", "hour"])
        .drop("ds", axis=1)
        .reset_index(drop=True)
    )
    # Yearly seasonality
    yearly_seasonality = forecast_system.iloc[: 24 * 400][["ds", "yearly"]].copy()
    yearly_seasonality["day"] = yearly_seasonality["ds"].apply(lambda x: x.day_of_year)
    yearly_seasonality["hour"] = yearly_seasonality["ds"].apply(lambda x: x.hour)
    yearly_seasonality["yearly"] = yearly_seasonality["yearly"].apply(
        lambda x: round(x, 5)
    )
    yearly_seasonality = (
        yearly_seasonality.drop_duplicates(["day", "hour"])
        .drop("ds", axis=1)
        .sort_values(["day", "hour"])
        .reset_index(drop=True)
    )
    # Holidays
    aux_holidays = forecast_system[list(holidays_df["holiday"].unique())].copy()
    aux_holidays = aux_holidays[aux_holidays.sum(axis=1) != 0].drop_duplicates()
    holidays_ser = aux_holidays.sum()
    holidays_names = holidays_ser.index
    for hol in holidays_names:
        holidays_ser[hol.split("[")[0].strip()] = holidays_ser[hol]
        holidays_ser.pop(hol)
    return (
        dayly_seasonalities_df,
        weekly_seasonality,
        yearly_seasonality,
        holidays_ser.to_dict(),
    )
