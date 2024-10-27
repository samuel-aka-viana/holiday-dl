import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict

import holidays
from conf.settings import LANDING_PATH, REGIOES
from conf.spark_session import SparkSessionManager
from pyspark.sql import DataFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class Holiday:
    data: str
    nome: str
    tipo: str
    estado: str
    dt_processamento: datetime
    dt_carga: datetime


class HolidayExtractor:
    def __init__(self):
        self.spark = SparkSessionManager.get_session()

    @staticmethod
    def _fetch_holidays(year: int, dt_processamento: datetime, dt_carga: datetime) -> List[Holiday]:
        logger.info("Fetching holidays for year: %s", year)
        holidays_list = []

        # Feriados nacionais
        br_holidays = holidays.country_holidays(country='BR', years=2024)

        for date, name in br_holidays.items():
            holidays_list.append(Holiday(
                data=date.strftime('%Y-%m-%d'),
                nome=name,
                tipo='Nacional',
                estado='ALL',
                dt_processamento=dt_processamento,
                dt_carga=dt_carga
            ))

        # Lista de estados brasileiros
        estados_brasileiros = [
            'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO',
            'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI',
            'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
        ]

        # Feriados estaduais
        for estado in estados_brasileiros:
            state_holidays = holidays.country_holidays(country='BR', state=estado)
            for date, name in state_holidays.items():
                if date.year == year:
                    holidays_list.append(Holiday(
                        data=date.strftime('%Y-%m-%d'),
                        nome=name,
                        tipo='Estadual',
                        estado=estado,
                        dt_processamento=dt_processamento,
                        dt_carga=dt_carga
                    ))

        logger.info("Fetched %d holidays", len(holidays_list))
        return holidays_list

    def _save_to_json(self, df: DataFrame, file_path: str) -> None:
        logger.info("Saving DataFrame to JSON at %s", file_path)
        df.write.mode("overwrite").json(file_path)

    def extract_to_landing(self) -> str:
        year = datetime.now().year
        dt_processamento = datetime.now()
        dt_carga = datetime.now()
        file_path = f"{LANDING_PATH}/holidays_{dt_carga.strftime('%Y-%m-%d')}.json"

        holidays_list = self._fetch_holidays(year, dt_processamento, dt_carga)
        holidays_df = self.spark.createDataFrame([h.__dict__ for h in holidays_list])

        self._save_to_json(holidays_df, file_path)
        return file_path


class HolidayExtractorOrchestrator:
    @staticmethod
    def run_extraction() -> Dict:
        extractor = HolidayExtractor()
        file_path = extractor.extract_to_landing()
        return {"file_path": file_path}
