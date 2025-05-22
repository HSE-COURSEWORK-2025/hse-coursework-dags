import os
import json
import requests
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

# Настраиваем логгер
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Читаем базовые URL из переменных окружения (подгружаются через ConfigMap / Secrets)
DATA_COLLECTION_API_BASE_URL = os.getenv(
    "DATA_COLLECTION_API_BASE_URL",
    "http://data-collection-api:8082"
)
AUTH_API_BASE_URL = os.getenv(
    "AUTH_API_BASE_URL",
    "http://auth-api:8081"
)
AUTH_API_FETCH_ALL_USERS_PATH = os.getenv(
    "AUTH_API_FETCH_ALL_USERS_PATH",
    "/auth-api/api/v1/internal/users/get_all_users"
)

# Общие аргументы DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 4, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=60),
}

@dag(
    dag_id="ml_analysis_k8s",
    default_args=default_args,
    schedule_interval=os.getenv("CRON_SCHEDULE_CHANNEL_DATA_UPDATE", "0 * * * *"),
    catchup=False,
    max_active_runs=1,
    tags=["user_processing", "kubernetes"],
)
def ml_analysis_dag():
    ...

dag_instance = ml_analysis_dag()
