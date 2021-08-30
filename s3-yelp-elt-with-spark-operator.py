from datetime import datetime, timedelta
from os import getenv

from airflow import DAG
from airflow.providers.amazon.aws.operators.s3_delete_objects import (
    S3DeleteObjectsOperator,
)
from airflow.providers.amazon.aws.operators.s3_list import S3ListOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)

PROCESSING_ZONE = getenv("PROCESSING_ZONE", "processing")
CURATED_ZONE = getenv("CURATED_ZONE", "curated")

default_args = {
    "owner": "Luba",
    "start_date": datetime(2021, 8, 31),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "s3-yelp-elt-with-spark-operator",
    default_args=default_args,
    schedule_interval="@daily",
    tags=["development", "s3", "sensor", "minio", "spark", "operator", "k8s"],
) as dag:

    # verify if new data has arrived on processing bucket
    # connecting to minio to check (sensor)
    verify_file_existence_processing = S3KeySensor(
        task_id="verify_file_existence_processing",
        bucket_name=PROCESSING_ZONE,
        bucket_key="pr-elt-business/*.json",
        wildcard_match=True,
        timeout=18 * 60 * 60,
        poke_interval=120,
        aws_conn_id="minio",
    )

    # use spark-on-k8s to operate against the data
    # containerized spark application
    # yaml definition to trigger process
    pr_elt_business_spark_operator = SparkKubernetesOperator(
        task_id="pr_elt_business_spark_operator",
        namespace="processing",
        application_file="dependencies/pr-elt-business.yaml",
        kubernetes_conn_id="minikube",
        do_xcom_push=True,
    )

    # monitor spark application
    # using sensor to determine the outcome of the task
    # read from xcom tp check the status [key & value] pair
    monitor_spark_app_status = SparkKubernetesSensor(
        task_id="monitor_spark_app_status",
        namespace="processing",
        application_name="{{ task_instance.xcom_pull(task_ids='pr_elt_business_spark_operator')['metadata']['name'] }}",
        kubernetes_conn_id="minikube",
    )

    # check if folder and file exists
    # delta zone for data lakehouse
    list_curated_s3_folder = S3ListOperator(
        task_id="list_curated_s3_folder",
        bucket=CURATED_ZONE,
        prefix="business/",
        delimiter="/",
        aws_conn_id="minio",
        do_xcom_push=True,
    )

    # remove files on folder
    delete_s3_file_processing_zone_2018 = S3DeleteObjectsOperator(
        task_id="delete_s3_file_processing_zone_2018",
        bucket=PROCESSING_ZONE,
        keys="pr-elt-business/yelp_academic_dataset_business_2018.json",
        aws_conn_id="minio",
        do_xcom_push=True,
    )

    # remove files on folder
    delete_s3_file_processing_zone_2019 = S3DeleteObjectsOperator(
        task_id="delete_s3_file_processing_zone_2019",
        bucket=PROCESSING_ZONE,
        keys="pr-elt-business/yelp_academic_dataset_business_2019.json",
        aws_conn_id="minio",
        do_xcom_push=True,
    )

    # tasks dependencies
    (
        verify_file_existence_processing
        >> pr_elt_business_spark_operator
        >> monitor_spark_app_status
        >> list_curated_s3_folder
        >> [delete_s3_file_processing_zone_2018, delete_s3_file_processing_zone_2019]
    )
