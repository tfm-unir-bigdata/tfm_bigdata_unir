import pendulum

from airflow.models.dag import DAG
# Este import funcionará después de actualizar el paquete
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitPySparkJobOperator,
)

# --- Constantes y Configuración ---
GCP_PROJECT_ID = "proyecto-tfm-unir"
GCP_REGION = "us-central1"
GCS_BUCKET = "amazon_pipeline"
PYTHON_FILES_PATH = f"gs://{GCS_BUCKET}/notebooks/jupyter"

# --- Configuración del Clúster ---
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "e2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
    "config_bucket": GCS_BUCKET,
    "software_config": {
        "image_version": "2.1-debian11",
        "optional_components": ["JUPYTER", "ZOOKEEPER"],
        "properties": {
            "spark:spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2"
        },
    },
    "initialization_actions": [
        {
            "executable_file": "gs://goog-dataproc-initialization-actions-us-west2/kafka/kafka.sh"
        }
    ],
    "endpoint_config": {"enable_http_port_access": True},
    "lifecycle_config": {"auto_delete_ttl": "14400s"},
}

# --- Argumentos por defecto del DAG ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# --- Definición del DAG ---
with DAG(
    dag_id="amazon_pipeline_orchestration_tfe", # Renombrado para la versión final
    start_date=pendulum.datetime(2025, 6, 28, tz="UTC"),
    catchup=False,
    schedule_interval=None,
    default_args=default_args,
    tags=['amazon', 'dataproc', 'pipeline'],
) as dag:

    # --- Definición de Tareas con el operador moderno ---
    bronze_get_datasets = DataprocSubmitPySparkJobOperator(
        task_id="bronze_get_datasets",
        main_python_file_uri=f"{PYTHON_FILES_PATH}/nb_get_datasets.py",
        cluster_config=CLUSTER_CONFIG, region=GCP_REGION, project_id=GCP_PROJECT_ID,
    )
    silver_dataquality_products = DataprocSubmitPySparkJobOperator(
        task_id="silver_dataquality_products",
        main_python_file_uri=f"{PYTHON_FILES_PATH}/nb_dq_datasets_products.py",
        cluster_config=CLUSTER_CONFIG, region=GCP_REGION, project_id=GCP_PROJECT_ID,
    )
    # ... (el resto de las tareas son iguales)
    silver_dataquality_reviews = DataprocSubmitPySparkJobOperator(
        task_id="silver_dataquality_reviews",
        main_python_file_uri=f"{PYTHON_FILES_PATH}/nb_dq_datasets_reviews.py",
        cluster_config=CLUSTER_CONFIG, region=GCP_REGION, project_id=GCP_PROJECT_ID,
    )
    gold_datamart_products = DataprocSubmitPySparkJobOperator(
        task_id="gold_datamart_products",
        main_python_file_uri=f"{PYTHON_FILES_PATH}/nb_dwh_products.py",
        cluster_config=CLUSTER_CONFIG, region=GCP_REGION, project_id=GCP_PROJECT_ID,
    )
    gold_datamart_reviews = DataprocSubmitPySparkJobOperator(
        task_id="gold_datamart_reviews",
        main_python_file_uri=f"{PYTHON_FILES_PATH}/nb_dwh_reviews.py",
        cluster_config=CLUSTER_CONFIG, region=GCP_REGION, project_id=GCP_PROJECT_ID,
    )

    # Dependencias:
    #
    # 1. La tarea bronze debe terminar ANTES de que las dos tareas silver comiencen.
    #    Las tareas silver pueden ejecutarse en paralelo entre sí.
    #       silver_dataquality_products -> bronze_get_datasets
    #       silver_dataquality_reviews -> bronze_get_datasets
    bronze_get_datasets >> [silver_dataquality_products, silver_dataquality_reviews]

    # 2. Las tareas silver deben terminar ANTES de que las tareas gold comiencen.
    #       gold_datamart_products -> silver_dataquality_products 
    #       gold_datamart_reviews -> silver_dataquality_reviews
    silver_dataquality_products >> gold_datamart_products
    silver_dataquality_reviews >> gold_datamart_reviews