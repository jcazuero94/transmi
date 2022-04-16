import pandas as pd
from flask import Flask
import dash
from dash.dependencies import Input, Output, State
from dash import dcc, html
from kedro.framework.startup import bootstrap_project
from pathlib import Path
from kedro.framework.session import KedroSession
from utilities import getMarks, unixTimeMillis

# app
app = dash.Dash(__name__)
server = app.server
# Constants
SEASONALITIES = [
    "Yearly seasonality",
    "Weekly seasonality",
    "Dayly seasonality",
    "Holidays",
]
# Kedro conection
metadata = bootstrap_project(Path.cwd().parent)
kedro_session = KedroSession.create(metadata.package_name, metadata.project_path)
kedro_context = kedro_session.load_context()
# Load data
forecast_df = kedro_context.catalog.load("forecast_system")
min_time = forecast_df["ds"].min()
max_time = forecast_df["ds"].max()

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
                            id="checklists",
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
                            value=[unixTimeMillis(min_time), unixTimeMillis(max_time)],
                            marks=getMarks(min_time, max_time),
                            id="date_slider",
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
                    [dcc.Graph(id="main_graph_errors")],
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
                    className="pretty_container six columns",
                ),
                html.Div(
                    [dcc.Graph(id="holidays_graph")],
                    className="pretty_container six columns",
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
    Output("main_graph", "figure"),
    [
        Input("stations_dropdown", "value"),
    ],
)
def make_main_figure(station):
    figure = {
        "data": [
            {
                "x": list(forecast_df["ds"]),
                "y": list(forecast_df["trend"]),
                "type": "line",
            }
        ],
        "layout": {"title": "Trend"},
    }
    return figure


# Main
if __name__ == "__main__":
    app.server.run(debug=True, threaded=True)