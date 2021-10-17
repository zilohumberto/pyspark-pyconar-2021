import os
import sys
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.utils.dates import days_ago

base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(base_dir))

from example_cluster import example_cluster_config


home = "/home/deploy"
venv_path = "/home/deploy/venv"

dag = DAG(
    "Spotify",
    description="Run a spotify job",
    default_args={"owner": "airflow", "depends_on_past": False},
    start_date=days_ago(0)
)

py_file = "spotify_analyzer.py"

copy_pyfile = BashOperator(
    task_id="copy_py",
    bash_command=f"source {venv_path}/bin/activate && databricks fs cp {home}/jobs/{py_file} dbfs:/FileStore/{py_file} --overwrite",
    dag=dag,
)

run_job = DatabricksSubmitRunOperator(
    task_id="spotify_task", json=example_cluster_config, dag=dag
)


copy_pyfile >> run_job
