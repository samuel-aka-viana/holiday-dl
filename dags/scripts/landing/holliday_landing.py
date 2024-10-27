import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, List

import holidays
from conf.settings import LANDING_PATH, REGIOES
from conf.spark_session import SparkSessionManager
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    lit,
    struct,
    array,
    col
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HolidayType(Enum):
    NACIONAL = "Nacional"
    ESTADUAL = "Estadual"


@dataclass
class Holiday:
    data: str
    nome: str
    tipo: HolidayType
    estado: str
    dt_processamento: str
    dt_carga: str

    def to_dict(self) -> Dict:
        return {
            'data': self.data,
            'nome': self.nome,
            'tipo': self.tipo.value,
            'estado': self.estado,
            'dt_processamento': self.dt_processamento,
            'dt_carga': self.dt_carga
        }


@dataclass
class ExtractionResult:
    file_path: str
    dt_carga: str
    total_records: int
    national_holidays: int
    state_holidays: int
    processing_time: float


class HolidayLanding:

    def __init__(self):
        self.spark = SparkSessionManager.get_session()
        self._initialize_schema()
        self.start_time = datetime.now()

    def _initialize_schema(self) -> None:
        self.schema = StructType([
            StructField("data", StringType(), True),
            StructField("nome", StringType(), True),
            StructField("tipo", StringType(), True),
            StructField("estado", StringType(), True),
            StructField("dt_processamento", TimestampType(), True),
            StructField("dt_carga", StringType(), True)
        ])

    def _get_national_holidays(self, year: int, dt_processamento: str, dt_carga: str) -> List[Holiday]:
        logger.info("Fetching national holidays for year: %s", year)
        holidays_list = []
        br_holidays = holidays.country_holidays(country='BR')

        for date, name in br_holidays.items():
            if date.year == year:
                holiday = Holiday(
                    data=date.strftime('%Y-%m-%d'),
                    nome=name,
                    tipo=HolidayType.NACIONAL,
                    estado='ALL',
                    dt_processamento=dt_processamento,
                    dt_carga=dt_carga
                )
                holidays_list.append(holiday)

        logger.info("Fetched %d national holidays", len(holidays_list))
        return holidays_list

    @staticmethod
    def _get_state_holidays(year: int, dt_processamento: str, dt_carga: str) -> List[Holiday]:
        logger.info("Fetching state holidays for year: %s", year)
        holidays_list = []
        for regiao, estados in REGIOES.items():
            for estado in estados:
                state_holidays = holidays.country_holidays(country='BR', state=estado)
                for date, name in state_holidays.items():
                    if date.year == year:
                        holiday = Holiday(
                            data=date.strftime('%Y-%m-%d'),
                            nome=name,
                            tipo=HolidayType.ESTADUAL,
                            estado=estado,
                            dt_processamento=dt_processamento,
                            dt_carga=dt_carga
                        )
                        holidays_list.append(holiday)

        logger.info("Fetched %d state holidays", len(holidays_list))
        return holidays_list

    def _create_spark_dataframe(self, holidays_list: List[Holiday]) -> DataFrame:
        logger.info("Creating Spark DataFrame with %d holidays", len(holidays_list))
        holidays_dict = [h.to_dict() for h in holidays_list]
        return self.spark.createDataFrame(holidays_dict, schema=self.schema)

    @staticmethod
    def _save_to_json(df: DataFrame, file_path: str, metadata: Dict) -> None:
        logger.info("Saving DataFrame to JSON at %s", file_path)
        json_df = df.select(
            struct(
                lit(metadata["dt_extracao"]).alias("dt_extracao"),
                lit(metadata["versao"]).alias("versao"),
                lit(metadata["fonte"]).alias("fonte"),
                lit(metadata["total_registros"]).alias("total_registros")
            ).alias("metadata"),
            array(
                struct(
                    col("data"),
                    col("nome"),
                    col("tipo"),
                    col("estado"),
                    col("dt_processamento"),
                    col("dt_carga")
                )
            ).alias("dados")
        )

        json_df.write.mode("overwrite").json(file_path)

    def extract_to_landing(self) -> ExtractionResult:
        try:
            year = datetime.now().year
            dt_processamento = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            dt_carga = datetime.now().strftime('%Y-%m-%d')
            file_path = f"{LANDING_PATH}/year={year}/holidays_{dt_carga}.json"

            national_holidays = self._get_national_holidays(year, dt_processamento, dt_carga)
            state_holidays = self._get_state_holidays(year, dt_processamento, dt_carga)

            all_holidays = national_holidays + state_holidays
            holidays_df = self._create_spark_dataframe(all_holidays)

            metadata = {
                "dt_extracao": datetime.now().isoformat(),
                "versao": "1.0",
                "fonte": "API Holidays",
                "total_registros": len(all_holidays)
            }

            self._save_to_json(holidays_df, file_path, metadata)

            processing_time = (datetime.now() - self.start_time).total_seconds()

            return ExtractionResult(
                file_path=file_path,
                dt_carga=dt_carga,
                total_records=len(all_holidays),
                national_holidays=len(national_holidays),
                state_holidays=len(state_holidays),
                processing_time=processing_time
            )

        except Exception as e:
            logger.error("Error during extraction: %s", str(e))
            raise
        finally:
            SparkSessionManager.stop_session()
            logger.info("Spark session stopped.")


class HolidayLandingOrchestrator:
    """Orquestrador para extração de feriados."""

    @staticmethod
    def extract_holidays() -> Dict:
        """Executa a extração dos feriados."""
        extractor = HolidayLanding()
        result = extractor.extract_to_landing()

        return {
            "file_path": result.file_path,
            "dt_carga": result.dt_carga,
            "metrics": {
                "total_records": result.total_records,
                "national_holidays": result.national_holidays,
                "state_holidays": result.state_holidays,
                "processing_time_seconds": result.processing_time
            }
        }
