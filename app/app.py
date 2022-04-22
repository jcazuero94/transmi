from datetime import date
from black import main
import pandas as pd
import dash
from dash.dependencies import Input, Output, State
from dash import dcc, html, dash_table
from kedro.framework.startup import bootstrap_project
from pathlib import Path
from kedro.framework.session import KedroSession
from utilities import getMarks, unixTimeMillis
import plotly.express as px
from sklearn import metrics


# app
app = dash.Dash(__name__)
server = app.server
# Constants
SEASONALITIES = [
    "Yearly seasonality",
    "Weekly seasonality",
    "Dayly seasonalities",
    "Holidays",
    "Schoolday",
]
LAYOUT_GRAPHS = {
    "paper_bgcolor": "rgba(255,255,255,100)",
    "plot_bgcolor": "rgba(255,255,255,100)",
}
XAXES_CONFIG = {
    "showgrid": True,
    "gridwidth": 1,
    "gridcolor": "lightgrey",
}
YAXES_CONFIG = {
    "showgrid": True,
    "gridwidth": 1,
    "gridcolor": "lightgrey",
}
YAXES_CONFIG_2 = {
    "zeroline": True,
    "zerolinewidth": 2,
    "zerolinecolor": "black",
}
DAY_OF_WEEK = {
    0: "Monday",
    1: "Tuesday",
    2: "Wednesday",
    3: "Thursday",
    4: "Friday",
    5: "Saturday",
    6: "Sunday",
}
# Kedro conection
metadata = bootstrap_project(Path.cwd().parent)
kedro_session = KedroSession.create(metadata.package_name, metadata.project_path)
kedro_context = kedro_session.load_context()
# Load data
forecast_df = kedro_context.catalog.load("forecast_system")
dayly_seasonalities = kedro_context.catalog.load("dayly_seasonalities_system")
weekly_seasonality = kedro_context.catalog.load("weekly_seasonality_system")
yearly_seasonality = kedro_context.catalog.load("yearly_seasonality_system")
holidays_ser = kedro_context.catalog.load("holidays_ser")
# Preparations
min_time = forecast_df["ds"].min()
max_time = forecast_df["ds"].max()
holidays_df = pd.DataFrame()
holidays_df["Holiday"] = list(holidays_ser.keys())
holidays_df["Value"] = list(holidays_ser.values())
holidays_df["Value"] = holidays_df["Value"].apply(lambda x: "{0:.0%}".format(x))
holidays_df.sort_values("Holiday", inplace=True)
yearly_seasonality = (
    yearly_seasonality[["yearly", "day"]].groupby("day").mean().reset_index()
)
yearly_seasonality.rename(
    {"day": "Day of year", "yearly": "Value"}, axis=1, inplace=True
)
dayly_seasonalities.rename({"day": "Day type"}, axis=1, inplace=True)
weekly_seasonality.rename({"weekly": "Value"}, axis=1, inplace=True)
weekly_seasonality["Day of week"] = weekly_seasonality["day"].map(DAY_OF_WEEK)
weekly_seasonality = (
    weekly_seasonality[["Value", "Day of week", "day"]]
    .groupby(["Day of week", "day"])
    .mean()
    .reset_index()
    .sort_values("day")
)
# Create app layout
app.layout = html.Div(
    [
        html.Div(
            [
                html.Div(
                    [
                        html.H2("Transmilenio demand", style={"padding-left": "10px"}),
                        html.H4(
                            "Hourly model including COVID impact",
                            style={"padding-left": "10px"},
                        ),
                    ],
                    className="ten columns",
                ),
                html.Div(
                    [
                        html.A(
                            html.Button("Learn More", id="learnMore"),
                            href="https://github.com/jcazuero94/transmilenio",
                        ),
                    ],
                    className="two columns",
                ),
            ],
            id="header",
            className="row",
        ),
        html.Div(
            [
                html.Div(
                    [
                        dcc.Dropdown(
                            options=[
                                {
                                    "label": "All stations",
                                    "value": "All stations",
                                }
                            ],
                            value="All stations",
                            id="stations_dropdown",
                            className="row",
                        ),
                        dcc.Checklist(
                            options=SEASONALITIES,
                            value=SEASONALITIES,
                            style={"padding-top": "10px"},
                            id="checklist",
                            inline=True,
                            className="row",
                            labelStyle={"padding-right": "10px"},
                        ),
                    ],
                    className="eight columns",
                    id="selector_col_1",
                ),
                html.Div(
                    [
                        dcc.RangeSlider(
                            min=unixTimeMillis(min_time),
                            max=unixTimeMillis(max_time),
                            value=[
                                unixTimeMillis(pd.Timestamp(year=2020, month=1, day=1)),
                                unixTimeMillis(max_time),
                            ],
                            marks=getMarks(min_time, max_time),
                            id="date_slider",
                            className="row",
                        ),
                        dcc.RadioItems(
                            options=["Hourly", "Dayly", "Monthly"],
                            value="Dayly",
                            style={"padding-left": "15px", "padding-top": "10px"},
                            labelStyle={"padding-right": "10px"},
                            id="demand_aggregation",
                            className="row",
                        ),
                    ],
                    className="four columns",
                    id="selector_col_2",
                ),
            ],
            className="row pretty_container",
            id="selectors_container",
        ),
        html.Div(
            [
                html.Div(
                    [dcc.Graph(id="main_graph")],
                    className="pretty_container eight columns",
                ),
                html.Div(
                    [
                        dcc.Graph(id="main_graph_errors", style={"height": "70%"}),
                        dash_table.DataTable(
                            id="errors_table",
                            style_header={
                                "backgroundColor": "white",
                                "fontWeight": "bold",
                            },
                            style_data={
                                "whiteSpace": "normal",
                                "height": "auto",
                            },
                            style_cell={
                                "width": "20%",
                                "textAlign": "center",
                            },
                            style_cell_conditional=[
                                {
                                    "if": {"column_id": "Metric"},
                                    "width": "40%",
                                    "textAlign": "center",
                                },
                            ],
                            columns=[
                                {"name": c, "id": c}
                                for c in ["Metric", "Value", "Pre-COVID", "COVID"]
                            ],
                        ),
                    ],
                    className="pretty_container four columns",
                ),
            ],
            className="row",
        ),
        html.Div(
            [
                html.Div(
                    [dcc.Graph(id="yearly_graph")],
                    className="pretty_container six columns",
                ),
                html.Div(
                    [dcc.Graph(id="weekly_graph")],
                    className="pretty_container six columns",
                ),
            ],
            className="row",
        ),
        html.Div(
            [
                html.Div(
                    [dcc.Graph(id="dayly_graph")],
                    className="pretty_container eight columns",
                ),
                html.Div(
                    [
                        dash_table.DataTable(
                            id="holidays_table",
                            sort_action="native",
                            style_header={
                                "backgroundColor": "white",
                                "fontWeight": "bold",
                            },
                            style_table={"height": "450px", "overflowY": "auto"},
                            style_data={
                                "whiteSpace": "normal",
                                "height": "auto",
                            },
                            fixed_rows={"headers": True, "data": 0},
                            style_cell_conditional=[
                                {
                                    "if": {"column_id": "Holiday"},
                                    "width": "60%",
                                    "textAlign": "center",
                                },
                                {
                                    "if": {"column_id": "Value"},
                                    "textAlign": "center",
                                },
                            ],
                        )
                    ],
                    className="pretty_container four columns",
                ),
            ],
            className="row",
        ),
    ],
    id="mainContainer",
    style={"display": "flex", "flex-direction": "column"},
)

# Callbacks
@app.callback(
    [
        Output("main_graph", "figure"),
        Output("errors_table", "data"),
        Output("main_graph_errors", "figure"),
    ],
    [
        Input("stations_dropdown", "value"),
        Input("checklist", "value"),
        Input("date_slider", "value"),
        Input("demand_aggregation", "value"),
    ],
)
def make_main_figure(station, checklist, date_range, demand_aggregation):
    ini_date = pd.Timestamp(date_range[0], unit="s")
    end_date = pd.Timestamp(date_range[1], unit="s")
    cols_prediction = []
    if "Yearly seasonality" in checklist:
        cols_prediction += ["yearly"]
    if "Weekly seasonality" in checklist:
        cols_prediction += ["weekly"]
    if "Dayly seasonalities" in checklist:
        cols_prediction += ["weekday", "saturday", "sunday", "holiday", "schoolday"]
    if "Holidays" in checklist:
        cols_prediction += ["holidays"]
    if "Schoolday" in checklist:
        cols_prediction += ["schoolday_reg"]
    forecast_df["yhat"] = 1
    for c in cols_prediction:
        forecast_df["yhat"] = forecast_df["yhat"] + forecast_df[c]
    forecast_df["yhat"] = (forecast_df["trend"] * forecast_df["yhat"]).apply(
        lambda x: max(x, 0)
    )
    main_fig_df = forecast_df[["ds", "y", "yhat"]].copy()
    main_fig_df.rename(
        {"ds": "Date", "y": "Observed", "yhat": "Modelled"}, axis=1, inplace=True
    )
    if demand_aggregation == "Dayly":
        main_fig_df["Date"] = main_fig_df["Date"].apply(lambda x: x.date())
        main_fig_df = main_fig_df.groupby("Date").sum().reset_index()
    if demand_aggregation == "Monthly":
        main_fig_df["Date"] = main_fig_df["Date"].apply(
            lambda x: pd.Timestamp(year=x.year, month=x.month, day=1)
        )
        main_fig_df = main_fig_df.groupby("Date").sum().reset_index()
    main_fig_df = main_fig_df[
        (main_fig_df["Date"] >= ini_date) & (main_fig_df["Date"] <= end_date)
    ].copy()
    main_fig_df_final = main_fig_df.melt(
        id_vars=["Date"], var_name="Type", value_name="Demand"
    )
    figure = px.line(
        main_fig_df_final,
        x="Date",
        y="Demand",
        color="Type",
        title="Observed and modelled demand",
    )
    figure.update_layout(**LAYOUT_GRAPHS)
    figure.update_xaxes(**XAXES_CONFIG)
    figure.update_yaxes(**YAXES_CONFIG)

    mets = pd.DataFrame(columns=["Metric", "Value", "Pre-COVID", "COVID"])
    main_fig_df.reset_index(inplace=True, drop=True)
    covid_date = pd.Timestamp(year=2020, month=3, day=20)
    main_fig_df_precov = main_fig_df[pd.to_datetime(main_fig_df["Date"]) <= covid_date]
    main_fig_df_cov = main_fig_df[pd.to_datetime(main_fig_df["Date"]) > covid_date]
    for i, metric in enumerate(
        zip(
            [
                metrics.mean_absolute_error,
                metrics.median_absolute_error,
                metrics.mean_squared_error,
            ],
            ["Mean Absolute Error", "Median Absolute Error", "Root Mean Squared Error"],
        )
    ):
        met = metric[0]
        met_name = metric[1]
        mets.loc[i] = [
            met_name,
            met(main_fig_df["Observed"], main_fig_df["Modelled"]),
            met(main_fig_df_precov["Observed"], main_fig_df_precov["Modelled"])
            if len(main_fig_df_precov) > 0
            else "NA",
            met(main_fig_df_cov["Observed"], main_fig_df_cov["Modelled"])
            if len(main_fig_df_cov) > 0
            else "NA",
        ]
    mets.iloc[0, 1:] = mets.iloc[0, 1:].map(
        lambda x: x if type(x) == str else int(round(x, -2))
    )
    mets.iloc[1, 1:] = mets.iloc[1, 1:].map(
        lambda x: x if type(x) == str else int(round(x, -2))
    )
    mets.iloc[2, 1:] = mets.iloc[2, 1:].map(
        lambda x: x if type(x) == str else int(round(x ** 0.5, -2))
    )
    figure_errors = px.scatter(main_fig_df, "Observed", "Modelled")
    figure_errors.update_layout(**LAYOUT_GRAPHS)
    figure_errors.update_xaxes(**XAXES_CONFIG)
    figure_errors.update_yaxes(**YAXES_CONFIG)
    return figure, mets.to_dict("records"), figure_errors


@app.callback(
    Output("dayly_graph", "figure"),
    [Input("stations_dropdown", "value"), Input("checklist", "value")],
)
def make_dayly_seasonalities(station, checklist):
    figure = px.line(
        dayly_seasonalities,
        x="hour",
        y="value",
        color="Day type",
        title="Dayly seasonalities",
    )
    figure.update_layout(yaxis_tickformat=",.0%", **LAYOUT_GRAPHS)
    figure.update_xaxes(**XAXES_CONFIG)
    figure.update_yaxes(**YAXES_CONFIG, **YAXES_CONFIG_2)
    return figure


@app.callback(
    Output("weekly_graph", "figure"),
    [Input("stations_dropdown", "value"), Input("checklist", "value")],
)
def make_weekly_seasonality(station, checklist):
    figure = px.line(
        weekly_seasonality,
        x="Day of week",
        y="Value",
        title="Weekly seasonality",
    )
    figure.update_layout(yaxis_tickformat=",.0%", **LAYOUT_GRAPHS)
    figure.update_xaxes(**XAXES_CONFIG)
    figure.update_yaxes(**YAXES_CONFIG, **YAXES_CONFIG_2)
    return figure


@app.callback(
    Output("yearly_graph", "figure"),
    [Input("stations_dropdown", "value"), Input("checklist", "value")],
)
def make_yearly_seasonality(station, checklist):
    figure = px.line(
        yearly_seasonality,
        x="Day of year",
        y="Value",
        title="Yearly seasonality",
    )
    figure.update_layout(
        xaxis=dict(
            tickmode="array",
            tickvals=[1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 365],
            ticktext=[
                "Jan",
                "Feb",
                "Mar",
                "Apr",
                "May",
                "Jun",
                "Jul",
                "Aug",
                "Sep",
                "Oct",
                "Nov",
                "Dic",
                "Jan",
            ],
            title="Month",
        ),
        yaxis_tickformat=",.0%",
        **LAYOUT_GRAPHS
    )
    figure.update_xaxes(**XAXES_CONFIG)
    figure.update_yaxes(**YAXES_CONFIG, **YAXES_CONFIG_2)
    return figure


@app.callback(
    [Output("holidays_table", "data"), Output("holidays_table", "columns")],
    [Input("stations_dropdown", "value"), Input("checklist", "value")],
)
def make_holidays_table(station, checklist):
    return holidays_df.to_dict("records"), [
        {"name": col, "id": col} for col in holidays_df.columns
    ]


# Main
if __name__ == "__main__":
    app.server.run(debug=True, threaded=True)