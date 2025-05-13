import os
import json
import requests
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.exceptions import AirflowException

# Читаем базовые URL из переменных окружения (подгружаются из .env)
DATA_COLLECTION_API_BASE_URL = os.getenv(
    "DATA_COLLECTION_API_BASE_URL",
    "http://localhost:8082"
)
AUTH_API_BASE_URL = os.getenv(
    "AUTH_API_BASE_URL",
    "http://localhost:8081"
)
# Путь к эндпоинту для получения всех пользователей
AUTH_API_FETCH_ALL_USERS_PATH = os.getenv(
    "AUTH_API_FETCH_ALL_USERS_PATH",
    "/auth-api/api/v1/internal/users/get_all_users"
)

def _url(path: str) -> str:
    """Собирает полный URL на переданный путь в нужном API"""
    # Для других API можно расширить логику по префиксу
    if path.startswith("/auth-api"):
        return f"{AUTH_API_BASE_URL}{path}"
    else:
        return f"{DATA_COLLECTION_API_BASE_URL}{path}"

# Общие аргументы DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 4, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=60),
}

@dag(
    dag_id="fetch_all_users_and_data",
    default_args=default_args,
    schedule_interval=os.getenv("CRON_SCHEDULE_CHANNEL_DATA_UPDATE", "0 * * * *"),
    catchup=False,
    max_active_runs=1,
    tags=["user_processing"],
)
def fetch_all_users_and_data_dag():
    @task(retries=2)
    def fetch_users():
        """Fetch all users from the authentication API"""
        url = _url(AUTH_API_FETCH_ALL_USERS_PATH)
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            users = response.json()
            if not users:
                raise AirflowException("No users found in the response")
            return users
        except requests.exceptions.RequestException as e:
            raise AirflowException(f"API request failed: {e}")

    def _create_docker_task(user: dict):
        """Helper function to create DockerOperator instance"""
        # Передаем внутрь контейнера URL через переменные окружения
        return DockerOperator(
            task_id=f"process_user_{user['email'].replace('@', '_at_')}",
            image="fetch_users:latest",
            api_version='auto',
            auto_remove=True,
            docker_url="unix://var/run/docker.sock",
            network_mode="host",
            environment={
                "DATA_COLLECTION_API_BASE_URL": DATA_COLLECTION_API_BASE_URL,
                "AUTH_API_BASE_URL": AUTH_API_BASE_URL,
                "AIRFLOW_UID": os.getenv("AIRFLOW_UID", "0"),
                "PYTHONUNBUFFERED": "1",
            },
            command=["--user-json", json.dumps(user, ensure_ascii=False)],
            mounts=[],
            retrieve_output=True,
        )

    @task
    def process_user(user: dict):
        """Wrapper task for Docker operator"""
        return _create_docker_task(user).execute({})

    users = fetch_users()
    process_user.expand(user=users)


dag_instance = fetch_all_users_and_data_dag()
