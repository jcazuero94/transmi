from kedro.pipeline import Pipeline, node
from transmi.pipelines.reporting.nodes import (
    prepare_hourly_system_model_app,
)


def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=prepare_hourly_system_model_app,
                inputs=["forecast_system", "holidays_df"],
                outputs=[
                    "dayly_seasonalities_system",
                    "weekly_seasonality_system",
                    "yearly_seasonality_system",
                    "holidays_ser",
                ],
                name="prepare_hourly_system_model_app",
                tags=["app_dash"],
            ),
        ]
    )
