import logging
import os
import sys
from typing import Optional

from conf.settings import SPARK_CONFIGS, DELTA_TABLE_CONFIGS
from pyspark import SparkConf
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class SparkSessionManager:
    _instance: Optional[SparkSession] = None

    @classmethod
    def get_session(cls, app_name: Optional[str] = None) -> SparkSession:
        if cls._instance is None:
            try:
                os.environ['PYSPARK_PYTHON'] = sys.executable
                os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

                conf = SparkConf()

                conf.setAppName(app_name or SPARK_CONFIGS.get("spark.app.name", "default-app"))

                for key, value in SPARK_CONFIGS.items():
                    if key != "spark.app.name":
                        conf.set(key, str(value))

                builder = SparkSession.builder.config(conf=conf)

                for key, value in DELTA_TABLE_CONFIGS.items():
                    builder = builder.config(f"spark.databricks.delta.{key}", str(value))

                cls._instance = builder.getOrCreate()

                logger.info("Sessão Spark criada com sucesso")
                logger.info(f"Spark Version: {cls._instance.version}")
                logger.info(f"Python Location: {sys.executable}")

            except Exception as e:
                error_msg = f"Erro ao criar sessão Spark: {str(e)}"
                logger.error(error_msg)
                logger.error(f"Python Path: {sys.executable}")
                logger.error(f"Environment: {os.environ}")
                raise Exception(error_msg)

        return cls._instance

    @classmethod
    def stop_session(cls) -> None:
        try:
            if cls._instance:
                cls._instance.catalog.clearCache()

                cls._instance.stop()
                cls._instance = None

                logger.info("Sessão Spark encerrada com sucesso")
        except Exception as e:
            logger.error(f"Erro ao encerrar sessão Spark: {str(e)}")
        finally:
            cls._instance = None


if __name__ == "__main__":
    try:
        spark = SparkSessionManager.get_session("TestApp")
        print("Spark session created successfully")

        df = spark.createDataFrame([(1, "test")], ["id", "value"])
        df.show()

    except Exception as e:
        print(f"Error testing Spark session: {str(e)}")
    finally:
        SparkSessionManager.stop_session()
        print("Spark session stopped successfully")
