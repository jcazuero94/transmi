from kedro.pipeline import Pipeline, node
from transmi.pipelines.extraction.nodes import (
    extraction_validaciones_troncal,
    extraction_summary_validaciones_troncal,
)


def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=extraction_validaciones_troncal,
                inputs=[
                    "params:start_validaciones_troncal",
                    "params:extraction_day",
                    "validaciones_troncal_log_raw",
                    "dic_estacion_linea_raw",
                ],
                outputs=[
                    "validaciones_troncal",
                    "validaciones_troncal_log_int",
                    "dic_estacion_linea_int",
                ],
                name="extraction_validaciones_troncal",
                tags=["extraction"],
            ),
            node(
                func=extraction_summary_validaciones_troncal,
                inputs=[
                    "links_data_transmi",
                ],
                outputs="validaciones_troncal_summary@pandas",
                name="extraction_summary_validaciones_troncal",
                tags=["extraction"],
            ),
        ]
    )
