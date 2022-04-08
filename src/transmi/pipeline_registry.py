"""Project pipelines."""
from typing import Dict

from kedro.pipeline import Pipeline, pipeline

from transmi.pipelines import extraction as ext
from transmi.pipelines import data_engineering as de


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.
    """
    extraction_pipeline = ext.create_pipeline()
    data_engineering_pipeline = de.create_pipeline()
    return {
        "__default__": extraction_pipeline + data_engineering_pipeline,
        "extraction": extraction_pipeline,
        "data_engineering": data_engineering_pipeline,
    }
