import pandas as pd
from flask import Flask
import dash
from dash.dependencies import Input, Output, State
from dash import dcc, html
from kedro.framework.startup import bootstrap_project
from pathlib import Path
from kedro.framework.session import KedroSession

# app
app = dash.Dash(__name__)
server = app.server
# Kedro conection
metadata = bootstrap_project(Path.cwd().parent)
kedro_session = KedroSession.create(metadata.package_name, metadata.project_path)
kedro_context = kedro_session.load_context()
# Load data
forecast_df = kedro_context.catalog.load("forecast_system")

# # Create global chart template
# mapbox_access_token = "pk.eyJ1IjoiamFja2x1byIsImEiOiJjajNlcnh3MzEwMHZtMzNueGw3NWw5ZXF5In0.fk8k06T96Ml9CLGgKmk81w"

# Create app layout
app.layout = html.Div(
    [
        html.Div(
            [
                html.Div(
                    [
                        html.H2(
                            "New York Oil and Gas",
                        ),
                        html.H4(
                            "Production Overview",
                        ),
                    ],
                    className="ten columns",
                ),
                html.A(
                    html.Button("Learn More", id="learnMore"),
                    href="https://github.com/jcazuero94/transmilenio",
                    className="two columns",
                ),
            ],
            id="header",
            className="row",
        ),
        html.Div(
            [
                dcc.Dropdown(
                    options=[{"label": "All", "value": "All"}],
                    value="All",
                    id="stations_dropdown",
                ),
            ],
            className="row",
        ),
        html.Div(
            [
                html.Div(
                    [dcc.Graph(id="main_graph")],
                    className="pretty_container eight columns",
                ),
                html.Div(
                    [dcc.Graph(id="individual_graph")],
                    className="pretty_container four columns",
                ),
            ],
            className="row",
        ),
    ],
    id="mainContainer",
    style={"display": "flex", "flex-direction": "column"},
)


@app.callback(
    Output("main_graph", "figure"),
    [
        Input("stations_dropdown", "value"),
    ],
)
def make_main_figure(station):
    print(forecast_df)
    figure = {
        "data": [
            {
                "x": list(range(len(forecast_df))),
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