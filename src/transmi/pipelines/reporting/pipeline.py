from kedro.pipeline import Pipeline, node
from transmi.pipelines.reporting.nodes import (
    prepare_hourly_system_model_app,
)


def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=prepare_hourly_system_model_app,
                inputs=[
                    "forecast_system",
                ],
                outputs=[
                    "dayly_seasonalities_system",
                    "weekly_seasonality",
                    "yearly_seasonality",
                ],
                name="prepare_hourly_system_model_app",
                tags=["app_dash"],
            ),
        ]
    )
