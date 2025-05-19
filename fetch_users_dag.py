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
    dag_id="fetch_all_users_and_data_k8s",
    default_args=default_args,
    schedule_interval=os.getenv("CRON_SCHEDULE_CHANNEL_DATA_UPDATE", "0 * * * *"),
    catchup=False,
    max_active_runs=1,
    tags=["user_processing", "kubernetes"],
)
def fetch_all_users_and_data_dag():
    def _url(path: str) -> str:
        full = f"{AUTH_API_BASE_URL if path.startswith('/auth-api') else DATA_COLLECTION_API_BASE_URL}{path}"
        logger.info(f"Constructed URL: %s", full)
        return full

    @task(retries=2)
    def fetch_users():
        url = _url(AUTH_API_FETCH_ALL_USERS_PATH)
        logger.info("Fetching users from URL: %s", url)
        try:
            resp = requests.get(url, timeout=10)
            logger.info("Received response: status=%s, body=%s", resp.status_code, resp.text[:200])
            resp.raise_for_status()
            users = resp.json()
            logger.info("Parsed %d users", len(users) if isinstance(users, list) else 0)
            if not users:
                raise AirflowException("No users found in the response")
            # Возвращаем список JSON-строк для динамического маппинга
            return [json.dumps(u, ensure_ascii=False) for u in users]
        except Exception as e:
            logger.error("Error fetching users: %s", str(e), exc_info=True)
            raise AirflowException(f"API request failed: {e}")

    users = fetch_users()

    # Базовый Pod оператор
    base_op = KubernetesPodOperator.partial(
        task_id="process_user",
        namespace="hse-coursework-health",
        image="fetch_users:latest",
        cmds=["python3", "run.py"],  # точка входа из Dockerfile
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy='Never',
    )

    # Динамический маппинг: создаём для каждого пользователя свой pod
    base_op.expand(
        env_vars=users.map(lambda u: {
            "DATA_COLLECTION_API_BASE_URL": DATA_COLLECTION_API_BASE_URL,
            "AUTH_API_BASE_URL": AUTH_API_BASE_URL,
            "PYTHONUNBUFFERED": "1",
        }),
        arguments=users.map(lambda u: ["--user-json", u]),
    )


dag_instance = fetch_all_users_and_data_dag()
