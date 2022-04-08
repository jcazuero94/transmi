from kedro.pipeline import Pipeline, node
from transmi.pipelines.data_engineering.nodes import (
    clean_summary_validaciones,
    system_hourly_demand,
)


def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=clean_summary_validaciones,
                inputs=[
                    "validaciones_troncal_summary@pandas",
                ],
                outputs="validaciones_troncal_summary_clean@pandas",
                name="clean_summary_validaciones",
                tags=["data_engineering"],
            ),
            node(
                func=system_hourly_demand,
                inputs=[
                    "validaciones_troncal_summary_clean@pandas",
                ],
                outputs="system_hourly_demand@pandas",
                name="system_hourly_demand",
                tags=["data_engineering"],
            ),
        ]
    )
