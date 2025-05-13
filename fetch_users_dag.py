import os
import json
import requests
from dataclasses import dataclass
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.exceptions import AirflowException

# Читаем базовые URL из переменных окружения (подгружаются через ConfigMap / Secrets)
DATA_COLLECTION_API_BASE_URL = os.getenv(
    "DATA_COLLECTION_API_BASE_URL",
    "http://192.168.0.180:8082"
)
AUTH_API_BASE_URL = os.getenv(
    "AUTH_API_BASE_URL",
    "http://192.168.0.180:8081"
)
AUTH_API_FETCH_ALL_USERS_PATH = os.getenv(
    "AUTH_API_FETCH_ALL_USERS_PATH",
    "/auth-api/api/v1/internal/users/get_all_users"
)

# Общие аргументы DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 4, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=60),
}

@dag(
    dag_id="fetch_all_users_and_data_k8s",
    default_args=default_args,
    schedule_interval=os.getenv("CRON_SCHEDULE_CHANNEL_DATA_UPDATE", "0 * * * *"),
    catchup=False,
    max_active_runs=1,
    tags=["user_processing", "kubernetes"],
)
def fetch_all_users_and_data_dag():
    def _url(path: str) -> str:
        if path.startswith("/auth-api"):
            return f"{AUTH_API_BASE_URL}{path}"
        return f"{DATA_COLLECTION_API_BASE_URL}{path}"

    @task(retries=2)
    def fetch_users():
        url = _url(AUTH_API_FETCH_ALL_USERS_PATH)
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            users = resp.json()
            if not users:
                raise AirflowException("No users found in the response")
            # Преобразуем в список для динамического маппинга
            return [json.dumps(u, ensure_ascii=False) for u in users]
        except Exception as e:
            raise AirflowException(f"API request failed: {e}")

    users = fetch_users()

    (KubernetesPodOperator
        .partial(
            namespace="airflow",
            image="fetch_users:latest",
            cmds=["python", "-m", "your_module_main"],  # замените на реальную команду внутри образа
            env_vars={
                "DATA_COLLECTION_API_BASE_URL": DATA_COLLECTION_API_BASE_URL,
                "AUTH_API_BASE_URL": AUTH_API_BASE_URL,
                "PYTHONUNBUFFERED": "1",
            },
            get_logs=True,
            is_delete_operator_pod=True,
        )
        .expand(
            name=[f"process_user_{json.loads(payload)['email'].replace('@', '_at_')}" for payload in users],
            arguments=[["--user-json", payload] for payload in users],
        )
    )

dag_instance = fetch_all_users_and_data_dag()
