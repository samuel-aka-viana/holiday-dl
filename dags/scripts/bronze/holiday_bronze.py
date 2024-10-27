from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, Optional

from conf.settings import BRONZE_PATH
from conf.spark_session import SparkSessionManager
from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    input_file_name,
    current_timestamp,
    col,
    lit,
    when
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType
)


class FileStatus(Enum):
    PROCESSED = "PROCESSED"
    ERROR = "ERROR"
    INVALID = "INVALID"


@dataclass
class FileInfo:
    file_path: str
    dt_carga: str
    partition_date: str


@dataclass
class ProcessingResult:
    status: FileStatus
    records_processed: int
    error_message: Optional[str] = None
    processing_time: float = 0.0
    partition_info: Optional[Dict[str, str]] = None


class HolidayBronze:

    def __init__(self):
        self.spark: SparkSession = SparkSessionManager.get_session()
        self._initialize_schema()
        self.start_time = datetime.now()

    def _initialize_schema(self) -> None:
        self.schema = StructType([
            StructField("data", StringType(), True),
            StructField("nome", StringType(), True),
            StructField("tipo", StringType(), True),
            StructField("estado", StringType(), True),
            StructField("dt_carga", StringType(), True),
            StructField("source_file", StringType(), True),
            StructField("bronze_ingestion_tsp", TimestampType(), True),
            StructField("status_processamento", StringType(), True),
            StructField("ano_particao", IntegerType(), True),
            StructField("mes_particao", IntegerType(), True)
        ])

    @staticmethod
    def _validate_json_structure(df: DataFrame) -> bool:
        required_columns = {"data", "nome", "tipo", "estado", "dt_carga"}
        return required_columns.issubset(df.columns)

    def _read_json(self, file_info: FileInfo) -> DataFrame:
        try:
            return self.spark.read \
                .option("multiLine", "true") \
                .json(file_info.file_path)
        except Exception as e:
            raise ValueError(f"Erro ao ler arquivo JSON: {str(e)}")

    @staticmethod
    def _enrich_dataframe(df: DataFrame, file_info: FileInfo) -> DataFrame:
        partition_date = datetime.strptime(file_info.partition_date, "%Y-%m-%d")

        return df \
            .withColumn("source_file", input_file_name()) \
            .withColumn("bronze_ingestion_tsp", current_timestamp()) \
            .withColumn("ano_particao", lit(partition_date.year)) \
            .withColumn("mes_particao", lit(partition_date.month)) \
            .withColumn("status_processamento",
                        when(col("_corrupt_record").isNull(), "VALID")
                        .otherwise("INVALID")
                        )

    def _save_to_bronze(self, df: DataFrame) -> None:
        try:
            if DeltaTable.isDeltaTable(self.spark, BRONZE_PATH):
                delta_table = DeltaTable.forPath(self.spark, BRONZE_PATH)

                delta_table.alias("target").merge(
                    df.alias("source"),
                    """
                    target.data = source.data AND 
                    target.estado = source.estado AND 
                    target.dt_carga = source.dt_carga
                    """
                ).whenMatchedUpdateAll(
                    condition="source.status_processamento = 'VALID'"
                ).whenNotMatchedInsertAll(
                    condition="source.status_processamento = 'VALID'"
                ).execute()
            else:
                df.write \
                    .format("delta") \
                    .partitionBy("ano_particao", "mes_particao") \
                    .mode("append") \
                    .save(BRONZE_PATH)

        except Exception as e:
            raise Exception(f"Erro ao salvar na camada Bronze: {str(e)}")

    def _generate_processing_result(
            self,
            df: DataFrame,
            status: FileStatus,
            error_msg: Optional[str] = None
    ) -> ProcessingResult:
        processing_time = (datetime.now() - self.start_time).total_seconds()

        return ProcessingResult(
            status=status,
            records_processed=df.count(),
            error_message=error_msg,
            processing_time=processing_time,
            partition_info={
                "ano": df.select("ano_particao").distinct().collect()[0][0],
                "mes": df.select("mes_particao").distinct().collect()[0][0]
            } if status == FileStatus.PROCESSED else None
        )

    def process_file(self, file_info: FileInfo) -> ProcessingResult:
        try:
            df = self._read_json(file_info)

            if not self._validate_json_structure(df):
                return self._generate_processing_result(
                    df,
                    FileStatus.INVALID,
                    "Estrutura do JSON inválida"
                )

            enriched_df = self._enrich_dataframe(df, file_info)
            self._save_to_bronze(enriched_df)

            return self._generate_processing_result(
                enriched_df,
                FileStatus.PROCESSED
            )

        except Exception as e:
            error_msg = f"Erro no processamento: {str(e)}"
            print(error_msg)
            return ProcessingResult(
                status=FileStatus.ERROR,
                records_processed=0,
                error_message=error_msg,
                processing_time=(datetime.now() - self.start_time).total_seconds()
            )
        finally:
            SparkSessionManager.stop_session()


class BronzeOrchestrator:

    @staticmethod
    def process_file(file_path: str, dt_carga: str, partition_date: str) -> Dict:
        file_info = FileInfo(
            file_path=file_path,
            dt_carga=dt_carga,
            partition_date=partition_date
        )

        processor = HolidayBronze()
        result = processor.process_file(file_info)

        return {
            "status": result.status.value,
            "records_processed": result.records_processed,
            "processing_time": result.processing_time,
            "error_message": result.error_message,
            "partition_info": result.partition_info
        }
