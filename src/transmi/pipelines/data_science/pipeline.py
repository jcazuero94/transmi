from kedro.pipeline import Pipeline, node
from transmi.pipelines.data_science.nodes import (
    system_model_fit,
)


def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=system_model_fit,
                inputs=[
                    "system_hourly_demand@pandas",
                    "params:quarantines",
                    "params:n_cv",
                ],
                outputs=[
                    "forecast_system",
                    "fig_components_system",
                    "fig_forecast_system",
                    "holidays_df",
                ],
                name="system_model_fit",
                tags=["data_science"],
            ),
        ]
    )
