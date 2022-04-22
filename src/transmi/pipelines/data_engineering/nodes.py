import pandas as pd
import holidays


def clean_summary_validaciones(validaciones_troncal_summary: pd.DataFrame):
    """Cleans dataset of summary validaciones"""
    validaciones_troncal_summary = validaciones_troncal_summary[
        validaciones_troncal_summary["Estación"].apply(lambda x: len(x.split(")")[0]))
        == 6
    ].copy()
    validaciones_troncal_summary["cod_est"] = validaciones_troncal_summary[
        "Estación"
    ].apply(lambda x: int(x.split(")")[0][1:]))
    validaciones_troncal_summary["date_hour"] = validaciones_troncal_summary.apply(
        lambda row: pd.Timestamp(
            year=row["date"].year,
            month=row["date"].month,
            day=row["date"].day,
            hour=row["Intervalo"].hour,
        ),
        axis=1,
    )
    validaciones_troncal_summary.drop(["Intervalo", "date"], axis=1, inplace=True)
    return validaciones_troncal_summary


def system_hourly_demand(validaciones_troncal_summary_clean: pd.DataFrame):
    """Creates hourly demand dataset of the system for imput of time series model"""
    demand_summary = (
        validaciones_troncal_summary_clean[["date_hour", "demand"]]
        .groupby("date_hour")
        .sum()
    )
    del validaciones_troncal_summary_clean
    start_range = min(demand_summary.index)
    end_range = max(demand_summary.index)
    complete_index = pd.date_range(start=start_range, end=end_range, freq="1H")
    dates_to_impute = [x for x in complete_index if x not in demand_summary.index]
    for d in dates_to_impute:
        demand_summary.loc[d, "demand"] = 0
    demand_summary = demand_summary.sort_index().reset_index()
    holidays_df = pd.DataFrame(
        holidays.Colombia(years=list(range(2015, 2025))).items()
    ).rename({0: "ds", 1: "holiday"}, axis=1)
    holidays_df = pd.concat(
        [
            holidays_df,
            pd.DataFrame(
                [
                    [x + pd.Timedelta(unit="D", value=1), "Holy saturday"]
                    for x in holidays_df[
                        holidays_df["holiday"] == "Viernes Santo [Good Friday]"
                    ]["ds"]
                ],
                columns=holidays_df.columns,
            ),
        ],
        ignore_index=True,
    )
    demand_summary["holiday"] = demand_summary["date_hour"].apply(
        lambda x: pd.Timestamp(x.date()) in list(holidays_df["ds"])
    )
    demand_summary["sunday"] = (
        demand_summary["date_hour"].apply(lambda x: x.weekday() == 6)
        & ~demand_summary["holiday"]
    )
    demand_summary["saturday"] = (
        demand_summary["date_hour"].apply(lambda x: x.weekday() == 5)
        & ~demand_summary["holiday"]
    )
    demand_summary["weekday"] = (
        demand_summary["date_hour"].apply(lambda x: x.weekday() < 5)
        & ~demand_summary["holiday"]
    )
    demand_summary.rename({"date_hour": "ds", "demand": "y"}, axis=1, inplace=True)

    return demand_summary
