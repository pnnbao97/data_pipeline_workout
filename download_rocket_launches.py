import json
import pathlib

import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="download_rocket_launches",
    description="Download rocket launches data from the SpaceX API",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None,
)

download_launches = BashOperator(
    task_id="download_launches",
    bash_command="curl -o /opt/airflow/data/launches.json -L 'https://ll.thespacedevs.com/2.3.1/launch/upcoming'",
    dag=dag,
)

def _get_pictures():
    # Ensure directory exists
    pathlib.Path("/opt/airflow/data/images").mkdir(parents=True, exist_ok=True)

    # Download pictures of rocket launches
    with open("/opt/airflow/data/launches.json", "r") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = pathlib.Path("/opt/airflow/data/images") / image_filename
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded: {image_filename}")
            except requests_exceptions.RequestException as e:
                print(f"Error downloading {image_url}: {e}")

get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag,
)

notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /opt/airflow/data/images 2>/dev/null | wc -l) images."',
    dag=dag,
)

download_launches >> get_pictures >> notify