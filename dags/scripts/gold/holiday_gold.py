from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List, Dict, Optional

from conf.settings import SILVER_PATH, GOLD_PATH, REGIOES
from conf.spark_session import SparkSessionManager
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, sum, when, year,
    current_timestamp, lit, count, avg
)


class ProcessingStatus(Enum):
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    PARTIAL = "PARTIAL"


@dataclass
class RegionMetrics:
    total_holidays: int
    working_days: int
    holiday_percentage: float
    processing_status: ProcessingStatus
    error_message: Optional[str] = None


class HolidayGold:
    def __init__(self):
        self.spark: SparkSession = SparkSessionManager.get_session()
        self.year: int = datetime.now().year
        self.processing_date: str = datetime.now().strftime('%Y-%m-%d')
        self._initialize_dataframes()

    def _initialize_dataframes(self) -> None:
        try:
            self.silver_df: DataFrame = self._load_silver_data()
            self.dates_df: DataFrame = self._create_date_sequence()
        except Exception as e:
            raise Exception(f"Erro ao inicializar DataFrames: {str(e)}")

    def _load_silver_data(self) -> DataFrame:
        df = self.spark.read.format("delta").load(SILVER_PATH)
        if df.isEmpty():
            raise ValueError("Sem dados na camada Silver")
        return df.filter(year(col("data")) == self.year)

    def _create_date_sequence(self) -> DataFrame:
        return self.spark.sql(f"""
            SELECT 
                date as data,
                dayofweek(date) as dia_semana,
                month(date) as mes,
                year(date) as ano
            FROM (
                SELECT explode(sequence(
                    to_date('{self.year}-01-01'), 
                    to_date('{self.year}-12-31'), 
                    interval 1 day
                )) as date
            )
        """)

    def _filter_regional_holidays(self, estados: List[str]) -> DataFrame:
        regional_df = self.silver_df.filter(
            (col("estado").isin(estados)) | (col("estado") == "ALL")
        )

        validated_df = regional_df.withColumn(
            "is_valid",
            when(
                col("data").isNotNull() &
                col("nome").isNotNull() &
                col("estado").isNotNull(),
                True
            ).otherwise(False)
        )

        return validated_df

    def _calculate_working_days(self, regional_df: DataFrame) -> DataFrame:
        calendar_df = self.dates_df.join(
            regional_df.filter(col("is_valid") == True),
            ["data"],
            "left_outer"
        )

        return calendar_df.select(
            col("data"),
            col("mes"),
            col("dia_semana"),
            when(
                col("nome").isNull() &
                (col("dia_semana").isin([1, 7]) == False),
                1
            ).otherwise(0).alias("dia_util"),
            when(col("nome").isNotNull(), 1)
            .otherwise(0).alias("is_holiday")
        )

    def _aggregate_monthly(self, working_days_df: DataFrame) -> DataFrame:
        return working_days_df.groupBy(col("mes")).agg(
            sum("dia_util").alias("dias_uteis"),
            sum("is_holiday").alias("total_feriados"),
            count("data").alias("total_dias"),
            avg("is_holiday").alias("percentual_feriados")
        ).withColumn(
            "ano",
            lit(self.year)
        ).withColumn(
            "dt_processamento",
            current_timestamp()
        ).orderBy("mes")

    def _save_to_gold(self, df: DataFrame, regiao: str) -> None:
        gold_path = f"{GOLD_PATH}/{regiao}"

        if DeltaTable.isDeltaTable(self.spark, gold_path):
            delta_table = DeltaTable.forPath(self.spark, gold_path)

            delta_table.alias("target").merge(
                df.alias("source"),
                """target.mes = source.mes AND 
                   target.ano = source.ano"""
            ) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
        else:
            df.write \
                .format("delta") \
                .partitionBy("ano") \
                .mode("overwrite") \
                .save(gold_path)

    def _create_temp_view(self, df: DataFrame, regiao: str) -> None:
        view_name = f"dias_uteis_{regiao}_{self.year}"
        df.createOrReplaceTempView(view_name)

    @staticmethod
    def _calculate_region_metrics(
            working_days_df: DataFrame,
            regiao: str
    ) -> RegionMetrics:
        metrics = working_days_df.agg(
            sum("is_holiday").alias("total_holidays"),
            sum("dia_util").alias("working_days"),
            (sum("is_holiday") / count("data") * 100).alias("holiday_percentage")
        ).collect()[0]

        return RegionMetrics(
            total_holidays=metrics["total_holidays"],
            working_days=metrics["working_days"],
            holiday_percentage=metrics["holiday_percentage"],
            processing_status=ProcessingStatus.SUCCESS
        )

    def process_region(self, regiao: str, estados: List[str]) -> RegionMetrics:
        try:
            regional_df = self._filter_regional_holidays(estados)

            if regional_df.filter(col("is_valid") == True).isEmpty():
                return RegionMetrics(
                    total_holidays=0,
                    working_days=0,
                    holiday_percentage=0,
                    processing_status=ProcessingStatus.ERROR,
                    error_message=f"Sem dados válidos para região {regiao}"
                )

            working_days = self._calculate_working_days(regional_df)
            monthly_working_days = self._aggregate_monthly(working_days)

            self._save_to_gold(monthly_working_days, regiao)
            self._create_temp_view(monthly_working_days, regiao)

            return self._calculate_region_metrics(working_days, regiao)

        except Exception as e:
            error_msg = f"Erro ao processar região {regiao}: {str(e)}"
            print(error_msg)
            return RegionMetrics(
                total_holidays=0,
                working_days=0,
                holiday_percentage=0,
                processing_status=ProcessingStatus.ERROR,
                error_message=error_msg
            )

    def process_all_regions(self) -> Dict[str, RegionMetrics]:
        results = {}
        try:
            for regiao, estados in REGIOES.items():
                results[regiao] = self.process_region(regiao, estados)

            all_success = all(
                m.processing_status == ProcessingStatus.SUCCESS
                for m in results.values()
            )

            if not all_success:
                print("Algumas regiões apresentaram erros no processamento")

            return results

        except Exception as e:
            print(f"Erro no processamento geral: {str(e)}")
            raise
        finally:
            SparkSessionManager.stop_session()


class GoldOrchestrator:

    @staticmethod
    def process_holidays() -> Dict[str, RegionMetrics]:
        processor = HolidayGold()
        return processor.process_all_regions()
