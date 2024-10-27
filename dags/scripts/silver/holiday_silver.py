from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict

from conf.settings import BRONZE_PATH, SILVER_PATH
from conf.spark_session import SparkSessionManager
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.connect.functions import initcap
from pyspark.sql.functions import (
    to_date, col, current_timestamp, lit, when,
    regexp_replace, trim, upper, year, month,
    count, sum, expr
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


class ProcessingStatus(Enum):
    SUCCESS = "SUCESSO"
    ERROR = "ERRO"
    PARTIAL = "PARCIAL"


@dataclass
class ValidationRule:
    name: str
    condition: str
    error_message: str


@dataclass
class ProcessingMetrics:
    total_records: int
    successful_records: int
    error_records: int
    processing_time: float
    validation_errors: Dict[str, int]
    processing_status: ProcessingStatus


class HolidaySilver:
    def __init__(self):
        self.spark: SparkSession = SparkSessionManager.get_session()
        self._initialize_schema()
        self._initialize_validation_rules()
        self.start_time = datetime.now()

    def _initialize_schema(self) -> None:
        self.schema = StructType([
            StructField("data", StringType(), True),
            StructField("nome", StringType(), True),
            StructField("tipo", StringType(), True),
            StructField("estado", StringType(), True),
            StructField("dt_processamento", TimestampType(), True),
            StructField("dt_carga", StringType(), True),
            StructField("fonte_dado", StringType(), True),
            StructField("status_processamento", StringType(), True),
            StructField("validation_errors", StringType(), True),
            StructField("last_updated", TimestampType(), True)
        ])

    def _initialize_validation_rules(self) -> None:
        self.validation_rules = [
            ValidationRule(
                name="data_not_null",
                condition="data is not null",
                error_message="Data não pode ser nula"
            ),
            ValidationRule(
                name="estado_valid",
                condition="estado is not null and length(estado) = 2",
                error_message="Estado inválido"
            ),
            ValidationRule(
                name="nome_not_empty",
                condition="nome is not null and length(trim(nome)) > 0",
                error_message="Nome do feriado não pode ser vazio"
            ),
            ValidationRule(
                name="data_format",
                condition="to_date(data, 'yyyy-MM-dd') is not null",
                error_message="Formato de data inválido"
            )
        ]

    def _load_source_data(self) -> DataFrame:
        df = self.spark.read.format("delta").load(BRONZE_PATH)

        if df.isEmpty():
            raise ValueError("Não há dados na camada Bronze para processar")

        return df.dropDuplicates(["data", "estado", "dt_carga"])

    def _validate_data(self, df: DataFrame) -> DataFrame:
        validation_conditions = []
        error_messages = []

        for rule in self.validation_rules:
            validation_conditions.append(
                f"when(not({rule.condition}), '{rule.error_message}')"
            )
            error_messages.append(rule.error_message)

        validation_expr = f"concat_ws(';', {','.join(validation_conditions)})"

        return df.withColumn(
            "validation_errors",
            expr(validation_expr)
        ).withColumn(
            "status_processamento",
            when(col("validation_errors") == "", ProcessingStatus.SUCCESS.value)
            .otherwise(ProcessingStatus.ERROR.value)
        )

    @staticmethod
    def _clean_data(df: DataFrame) -> DataFrame:
        return df \
            .withColumn("nome", regexp_replace("nome", "\s+", " ")) \
            .withColumn("nome", trim("nome")) \
            .withColumn("estado", upper("estado")) \
            .withColumn("tipo", initcap("tipo")) \
            .withColumn("data", regexp_replace("data", "/", "-"))

    @staticmethod
    def _transform_data(df: DataFrame) -> DataFrame:
        return df \
            .withColumn("data", to_date(col("data"))) \
            .withColumn("silver_timestamp", current_timestamp()) \
            .withColumn("fonte_dado", lit("API_HOLIDAYS")) \
            .withColumn("ano", year(col("data"))) \
            .withColumn("mes", month(col("data"))) \
            .withColumn("last_updated", current_timestamp())

    def _save_to_silver(self, df: DataFrame) -> None:
        try:
            if DeltaTable.isDeltaTable(self.spark, SILVER_PATH):
                delta_table = DeltaTable.forPath(self.spark, SILVER_PATH)

                delta_table.alias("target").merge(
                    df.alias("source"),
                    """
                    target.data = source.data AND 
                    target.estado = source.estado AND
                    target.dt_carga = source.dt_carga
                    """
                ) \
                    .whenMatchedUpdate(
                    condition="source.status_processamento = 'SUCESSO'",
                    set={
                        "nome": "source.nome",
                        "tipo": "source.tipo",
                        "silver_timestamp": "source.silver_timestamp",
                        "status_processamento": "source.status_processamento",
                        "validation_errors": "source.validation_errors",
                        "last_updated": "source.last_updated"
                    }
                ) \
                    .whenNotMatchedInsert(
                    values={
                        "data": "source.data",
                        "nome": "source.nome",
                        "tipo": "source.tipo",
                        "estado": "source.estado",
                        "dt_processamento": "source.dt_processamento",
                        "dt_carga": "source.dt_carga",
                        "fonte_dado": "source.fonte_dado",
                        "silver_timestamp": "source.silver_timestamp",
                        "status_processamento": "source.status_processamento",
                        "validation_errors": "source.validation_errors",
                        "last_updated": "source.last_updated",
                        "ano": "source.ano",
                        "mes": "source.mes"
                    }
                ) \
                    .execute()
            else:
                df.write \
                    .format("delta") \
                    .partitionBy("ano", "mes") \
                    .mode("append") \
                    .save(SILVER_PATH)

        except Exception as e:
            raise Exception(f"Erro ao salvar na camada Silver: {str(e)}")

    def _generate_metrics(self, df: DataFrame) -> ProcessingMetrics:
        metrics_df = df.agg(
            count("*").alias("total_records"),
            sum(when(col("status_processamento") == ProcessingStatus.SUCCESS.value, 1).otherwise(0)).alias(
                "successful_records"),
            sum(when(col("status_processamento") == ProcessingStatus.ERROR.value, 1).otherwise(0)).alias(
                "error_records")
        ).first()

        validation_errors = {}
        for rule in self.validation_rules:
            error_count = df.filter(
                col("validation_errors").contains(rule.error_message)
            ).count()
            validation_errors[rule.name] = error_count

        processing_time = (datetime.now() - self.start_time).total_seconds()

        if metrics_df["error_records"] == 0:
            status = ProcessingStatus.SUCCESS
        elif metrics_df["successful_records"] == 0:
            status = ProcessingStatus.ERROR
        else:
            status = ProcessingStatus.PARTIAL

        return ProcessingMetrics(
            total_records=metrics_df["total_records"],
            successful_records=metrics_df["successful_records"],
            error_records=metrics_df["error_records"],
            processing_time=processing_time,
            validation_errors=validation_errors,
            processing_status=status
        )

    def transform(self) -> Dict[str, any]:
        try:
            source_df = self._load_source_data()

            processed_df = source_df \
                .transform(self._clean_data) \
                .transform(self._transform_data) \
                .transform(self._validate_data)

            self._save_to_silver(processed_df)

            metrics = self._generate_metrics(processed_df)

            return {
                "status": metrics.processing_status.value,
                "metrics": {
                    "total_records": metrics.total_records,
                    "successful_records": metrics.successful_records,
                    "error_records": metrics.error_records,
                    "processing_time_seconds": metrics.processing_time,
                    "validation_errors": metrics.validation_errors
                },
                "metadata": self._get_processing_metadata()
            }

        except Exception as e:
            error_msg = f"Erro na transformação dos dados: {str(e)}"
            print(error_msg)
            return {
                "status": ProcessingStatus.ERROR.value,
                "error_message": error_msg,
                "metadata": self._get_processing_metadata()
            }
        finally:
            SparkSessionManager.stop_session()


class SilverOrchestrator:

    @staticmethod
    def process_holidays() -> Dict[str, any]:
        """Executa o processamento dos feriados."""
        processor = HolidaySilver()
        return processor.transform()
