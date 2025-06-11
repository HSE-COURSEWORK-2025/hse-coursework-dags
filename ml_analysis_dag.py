import os
import requests
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DATA_COLLECTION_API_BASE_URL = os.getenv("DATA_COLLECTION_API_BASE_URL", "http://data-collection-api:8082")
AUTH_API_BASE_URL            = os.getenv("AUTH_API_BASE_URL", "http://auth-api:8081")
AUTH_API_FETCH_ALL_USERS_PATH= os.getenv("AUTH_API_FETCH_ALL_USERS_PATH",
                                        "/auth-api/api/v1/internal/users/get_all_users?test_users=true&real_users=true")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=60),
}

@dag(
    dag_id="ml_analysis_k8s",
    default_args=default_args,
    schedule_interval="0 * * * *",
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=["ml", "kubernetes"],
)
def predict_risks_per_user_dag():
    def _url(path: str) -> str:
        return f"{AUTH_API_BASE_URL if path.startswith('/auth-api') else DATA_COLLECTION_API_BASE_URL}{path}"

    @task(retries=2)
    def fetch_users():
        url = _url(AUTH_API_FETCH_ALL_USERS_PATH)
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        users = resp.json()
        if not isinstance(users, list) or not users:
            raise AirflowException("No users returned")
        return [u["email"] for u in users if "email" in u]

    @task
    def build_args(emails: list[str]) -> list[list[str]]:
        return [["--email", email] for email in emails]

    user_emails = fetch_users()
    args_list   = build_args(user_emails)

    KubernetesPodOperator.partial(
        task_id="run_ml_predictions",
        namespace="hse-coursework-health",
        image="predict_using_ml:latest",            # образ с run_ml.py
        cmds=["python3", "run.py"],
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy="Never",
    ).expand(
        arguments=args_list
    )

dag_instance = predict_risks_per_user_dag()
