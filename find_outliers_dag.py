import os
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

# Настраиваем логгер
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Читаем базовые URL из переменных окружения
DATA_COLLECTION_API_BASE_URL = os.getenv(
    "DATA_COLLECTION_API_BASE_URL",
    "http://data-collection-api:8082"
)
AUTH_API_BASE_URL = os.getenv(
    "AUTH_API_BASE_URL",
    "http://auth-api:8081"
)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 4, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=60),
}

@dag(
    dag_id="find_outliers_k8s",
    default_args=default_args,
    schedule_interval=os.getenv("CRON_SCHEDULE_CHANNEL_DATA_UPDATE", "0 * * * *"),
    catchup=False,
    max_active_runs=1,
    tags=["user_processing", "kubernetes"],
)
def find_outliers_dag():
    def _url(path: str) -> str:
        base = AUTH_API_BASE_URL if path.startswith('/auth-api') else DATA_COLLECTION_API_BASE_URL
        full = f"{base}{path}"
        logger.info("Constructed URL: %s", full)
        return full

    find_outliers_task = KubernetesPodOperator(
        task_id="find_outliers",
        namespace="hse-coursework-health",
        image="find_outliers:latest",
        cmds=["python3", "run.py"],
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy='Never',
        # при необходимости можно передать env vars:
        # env_vars={"AUTH_URL": _url("/auth-api/..."), ...},
    )

    return find_outliers_task

dag_instance = find_outliers_dag()
