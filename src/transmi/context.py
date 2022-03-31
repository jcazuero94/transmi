from typing import Any, Dict, Union
from pathlib import Path

from pyspark import SparkConf
from pyspark.sql import SparkSession

from kedro.framework.context import KedroContext


class CustomContext(KedroContext):
    def __init__(
        self,
        package_name: str,
        project_path: Union[Path, str],
        env: str = None,
        extra_params: Dict[str, Any] = None,
    ):
        super().__init__(package_name, project_path, env, extra_params)
        self.init_spark_session()

    def init_spark_session(self) -> None:
        """Initialises a SparkSession using the config defined in project's conf folder."""

        # Load the spark configuration in spark.yaml using the config loader
        parameters = self.config_loader.get("spark*", "spark*/**")
        spark_conf = SparkConf().setAll(parameters.items())

        # Initialise the spark session
        spark_session_conf = (
            SparkSession.builder.appName(self.package_name)
            .master("local[5]")
            .config(conf=spark_conf)
        )
        _spark_session = spark_session_conf.getOrCreate()
        _spark_session.sparkContext.setLogLevel("WARN")
