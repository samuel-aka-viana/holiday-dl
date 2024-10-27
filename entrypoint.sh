#!/bin/bash

directories=(
    "/opt/airflow/logs"
    "/opt/airflow/logs/scheduler"
    "/opt/airflow/logs/dag_processor_manager"
    "/opt/airflow/logs/webserver"
    "/opt/airflow/logs/task_handler"
    "/opt/airflow/dags"
    "/opt/airflow/plugins"
    "/opt/airflow/working_dirs"
)

for dir in "${directories[@]}"; do
    mkdir -p "${dir}"
    chown -R airflow:root "${dir}"
    chmod -R g+rw "${dir}"
done

exec "$@"
