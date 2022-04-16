import pandas as pd


def prepare_hourly_system_model_app(forecast_system: pd.DataFrame):
    """Prepare forecast_system for app vsualization"""
    # Dayly seasonalities
    dayly_seasonalities = ["weekday", "saturday", "sunday", "holiday"]
    dayly_seasonalities_df = None
    for col in dayly_seasonalities:
        aux_df = forecast_system[["ds", col]].iloc[: 180 * 24].copy()
        aux_df["hour"] = aux_df["ds"].apply(lambda x: x.hour)
        aux_df.drop("ds", axis=1, inplace=True)
        aux_df = aux_df[aux_df[col] != 0].copy()
        aux_df[col] = aux_df[col].apply(lambda x: round(x, 5))
        aux_df.drop_duplicates(inplace=True)
        dayly_seasonalities_df = (
            aux_df
            if dayly_seasonalities_df is None
            else pd.merge(dayly_seasonalities_df, aux_df, on="hour")
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

    return dayly_seasonalities_df, weekly_seasonality, yearly_seasonality