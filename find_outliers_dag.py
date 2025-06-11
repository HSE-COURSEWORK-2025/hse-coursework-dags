import os
import json
import requests
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

# Логгер
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Конфиги
DATA_COLLECTION_API_BASE_URL = os.getenv("DATA_COLLECTION_API_BASE_URL", "http://data-collection-api:8082")
AUTH_API_BASE_URL = os.getenv("AUTH_API_BASE_URL", "http://auth-api:8081")
AUTH_API_FETCH_ALL_USERS_PATH = os.getenv("AUTH_API_FETCH_ALL_USERS_PATH", "/auth-api/api/v1/internal/users/get_all_users?test_users=true&real_users=true")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=60),
}

@dag(
    dag_id="find_outliers_per_user_k8s",
    default_args=default_args,
    schedule_interval="0 * * * *",
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=["user_processing", "kubernetes"],
)
def find_outliers_per_user_dag():
    def _url(path: str) -> str:
        return f"{AUTH_API_BASE_URL if path.startswith('/auth-api') else DATA_COLLECTION_API_BASE_URL}{path}"

    @task(retries=2)
    def fetch_users():
        url = _url(AUTH_API_FETCH_ALL_USERS_PATH)
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        users = resp.json()
        if not isinstance(users, list) or not users:
            raise AirflowException("No users found")
        # возвращаем список email’ов
        return [u["email"] for u in users if "email" in u]

    @task
    def build_args(emails: list[str]) -> list[list[str]]:
        """
        Из списка email’ов строит список аргументов для каждого пода:
          [['--email','a@b'], ['--email','c@d'], …]
        """
        return [["--email", email] for email in emails]

    # 1) Получаем XComArg со списком email’ов
    user_emails = fetch_users()

    # 2) Строим XComArg со списком аргументов
    args_list = build_args(user_emails)

    # 3) Запускаем один pod на каждый элемент args_list
    KubernetesPodOperator.partial(
        task_id="find_outliers",
        namespace="hse-coursework-health",
        image="find_outliers:latest",
        cmds=["python3", "run.py"],
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy="Never",
    ).expand(
        arguments=args_list
    )

dag_instance = find_outliers_per_user_dag()
