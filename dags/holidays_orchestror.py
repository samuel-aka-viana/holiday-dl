from datetime import datetime, timedelta
from typing import Dict

from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

from scripts.landing.holliday_landing import HolidayLandingOrchestrator
from scripts.bronze.holiday_bronze import BronzeOrchestrator
from scripts.gold.holiday_gold import GoldOrchestrator
from scripts.silver.holiday_silver import SilverOrchestrator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

@dag(
    dag_id="holiday_etl_pipeline",
    default_args=default_args,
    description="Pipeline ETL completo para processamento de feriados",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["holidays", "etl", "datalake"]
)
def holiday_pipeline():
    """
    Pipeline completo de processamento de feriados.

    Flow:
    Landing (JSON) -> Bronze (Delta) -> Silver (Delta) -> Gold (Delta)
    """

    @task(task_id="extract_to_landing")
    def extract_holidays() -> Dict:
        """Extrai feriados para landing zone em JSON."""
        return HolidayLandingOrchestrator.extract_holidays()

    @task(task_id="load_to_bronze")
    def process_bronze(landing_result: Dict) -> Dict:
        """Processa dados da landing zone para bronze."""
        return BronzeOrchestrator.process_file(
            file_path=landing_result["file_path"],
            dt_carga=landing_result["dt_carga"],
            partition_date=landing_result["dt_carga"]
        )

    @task(task_id="transform_to_silver")
    def process_silver(bronze_result: Dict) -> Dict:
        """Transforma dados da bronze para silver."""
        if bronze_result["status"] != "PROCESSED":
            raise Exception(
                f"Erro no processamento Bronze: {bronze_result['error_message']}"
            )
        return SilverOrchestrator.process_holidays()

    @task(task_id="create_gold_views")
    def process_gold(silver_result: Dict) -> Dict:
        """Processa dados da silver para gold."""
        if silver_result["status"] != "SUCCESS":
            raise Exception(
                f"Erro no processamento Silver: {silver_result['error_message']}"
            )
        return GoldOrchestrator.process_holidays()

    @task(
        task_id="process_metrics",
        trigger_rule=TriggerRule.ALL_DONE  # Executa mesmo se tasks anteriores falharem
    )
    def consolidate_metrics(
            landing_result: Dict,
            bronze_result: Dict,
            silver_result: Dict,
            gold_result: Dict
    ) -> Dict:
        """Consolida métricas de todas as camadas."""
        return {
            "execution_date": datetime.now().isoformat(),
            "landing_metrics": {
                "total_records": landing_result["metrics"]["total_records"],
                "national_holidays": landing_result["metrics"]["national_holidays"],
                "state_holidays": landing_result["metrics"]["state_holidays"],
                "processing_time": landing_result["metrics"]["processing_time_seconds"]
            },
            "bronze_metrics": {
                "status": bronze_result["status"],
                "records_processed": bronze_result["records_processed"],
                "processing_time": bronze_result["processing_time"]
            },
            "silver_metrics": {
                "status": silver_result["status"],
                "total_records": silver_result["metrics"]["total_records"],
                "successful_records": silver_result["metrics"]["successful_records"],
                "error_records": silver_result["metrics"]["error_records"]
            },
            "gold_metrics": {
                "regions_processed": len(gold_result),
                "status": "SUCCESS" if all(
                    m["status"] == "SUCCESS" for m in gold_result.values()
                ) else "PARTIAL",
                "metrics_by_region": {
                    region: {
                        "total_holidays": metrics["total_holidays"],
                        "working_days": metrics["working_days"]
                    }
                    for region, metrics in gold_result.items()
                }
            }
        }

    # Define o fluxo de execução
    landing_result = extract_holidays()
    bronze_result = process_bronze(landing_result)
    silver_result = process_silver(bronze_result)
    gold_result = process_gold(silver_result)

    consolidate_metrics(
        landing_result,
        bronze_result,
        silver_result,
        gold_result
    )

holiday_etl_dag = holiday_pipeline()
