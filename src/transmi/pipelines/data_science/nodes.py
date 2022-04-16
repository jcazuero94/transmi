import sys
from typing import List
import pandas as pd
import numpy as np
import datetime
import holidays
import fbprophet as prophet
import matplotlib.pyplot as plt
from sklearn.metrics import mean_squared_error
from transmi.pipelines.data_science.utilities import _chgs_from_base
from transmi.extras.supress_stdout_stderr import SuppressStdoutStderr


def system_model_fit(
    system_hourly_demand: pd.DataFrame,
    quarantines: List,
    n_cv: int,
):
    """Estimates model for the entire system"""
    quarantines = [pd.Timestamp(x) for x in quarantines]
    holidays_df = pd.DataFrame(
        holidays.Colombia(years=list(range(2015, 2025))).items()
    ).rename({0: "ds", 1: "holiday"}, axis=1)
    start_date = system_hourly_demand["ds"].min()
    end_date = system_hourly_demand["ds"].max()
    train_demand = system_hourly_demand[
        (system_hourly_demand["ds"] > start_date + pd.Timedelta(days=200))
        & (system_hourly_demand["ds"] < end_date - pd.Timedelta(days=60))
    ]
    cv_demand = system_hourly_demand[
        (system_hourly_demand["ds"] <= start_date + pd.Timedelta(days=200))
        | (system_hourly_demand["ds"] >= end_date - pd.Timedelta(days=60))
    ].copy()
    start_chgs = train_demand["ds"].min()
    end_chgs = train_demand["ds"].max()
    chgs = _chgs_from_base(quarantines, start_chgs, end_chgs)
    # Cross validation
    cv_error_best = np.inf
    sps_final = None
    hps_final = None
    for i in range(n_cv):
        print(f"Iteration {i} - {datetime.datetime.today()}")
        sps, hps = tuple(10 ** (np.random.rand(2) * 6 - 3))
        model = prophet.Prophet(
            interval_width=0.95,
            yearly_seasonality=True,
            daily_seasonality=False,
            weekly_seasonality=True,
            seasonality_mode="multiplicative",
            changepoints=chgs,
            changepoint_prior_scale=50,
            holidays_prior_scale=hps,
            seasonality_prior_scale=sps,
            holidays=holidays_df,
        )
        model.add_seasonality(
            name="weekday", period=1, fourier_order=9, condition_name="weekday"
        )
        model.add_seasonality(
            name="sunday", period=1, fourier_order=7, condition_name="sunday"
        )
        model.add_seasonality(
            name="saturday", period=1, fourier_order=7, condition_name="saturday"
        )
        model.add_seasonality(
            name="holiday", period=1, fourier_order=7, condition_name="holiday"
        )
        with SuppressStdoutStderr():
            model.fit(train_demand)
        pred_cv = model.predict(cv_demand)[["ds", "yhat"]]
        pred_cv["yhat"] = pred_cv["yhat"].apply(lambda x: max(x, 0))
        cv_demand = pd.merge(cv_demand, pred_cv, how="left", on="ds")
        cv_error = mean_squared_error(cv_demand["y"], cv_demand["yhat"]) ** 0.5
        cv_demand.drop("yhat", axis=1, inplace=True)
        if cv_error < cv_error_best:
            sps_final = sps
            hps_final = hps
            cv_error_best = cv_error
            print(f"RMSE: {cv_error}")

    start_chgs = system_hourly_demand["ds"].min()
    end_chgs = system_hourly_demand["ds"].max()
    chgs = _chgs_from_base(quarantines, start_chgs, end_chgs)
    model = prophet.Prophet(
        interval_width=0.95,
        yearly_seasonality=True,
        daily_seasonality=False,
        weekly_seasonality=True,
        seasonality_mode="multiplicative",
        changepoints=chgs,
        changepoint_prior_scale=50,
        holidays_prior_scale=hps_final,
        seasonality_prior_scale=sps_final,
        holidays=holidays_df,
    )
    model.add_seasonality(
        name="weekday", period=1, fourier_order=9, condition_name="weekday"
    )
    model.add_seasonality(
        name="sunday", period=1, fourier_order=7, condition_name="sunday"
    )
    model.add_seasonality(
        name="saturday", period=1, fourier_order=7, condition_name="saturday"
    )
    model.add_seasonality(
        name="holiday", period=1, fourier_order=7, condition_name="holiday"
    )
    model.fit(system_hourly_demand)
    fig_components = model.plot_components(model.predict())
    forecast = model.predict()
    fig_forecast, ax = plt.subplots()
    ax.plot(system_hourly_demand["ds"], system_hourly_demand["y"], label="Observado")
    ax.plot(
        forecast["ds"],
        forecast["yhat"].apply(lambda x: max(x, 0)),
        label="Modelado",
        color="orange",
    )
    fig_forecast.set_size_inches(400, 8)
    forecast = pd.merge(
        forecast,
        system_hourly_demand[["ds", "y", "weekday", "sunday", "saturday", "holiday"]],
        how="left",
        on="ds",
        suffixes=["", "_mark"],
    )
    print(forecast.columns)
    return forecast, fig_components, fig_forecast