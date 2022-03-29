"""Project pipelines."""
from typing import Dict

from kedro.pipeline import Pipeline, pipeline

from transmi.pipelines import extraction as ext


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.
    """
    extraction_pipeline = ext.create_pipeline()
    return {"__default__": extraction_pipeline, "extraction": extraction_pipeline}
