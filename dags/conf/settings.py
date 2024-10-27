MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "DUMMYIDEXAMPLE"
MINIO_SECRET_KEY = "DUMMYEXAMPLEKEY"
GCS_BUCKET = "datalake"

BASE_PATH = f"s3a://{GCS_BUCKET}"
LANDING_PATH = f"{BASE_PATH}/landing/holidays"
BRONZE_PATH = f"{BASE_PATH}/bronze/holidays"
SILVER_PATH = f"{BASE_PATH}/silver/holidays"
GOLD_PATH = f"{BASE_PATH}/gold/holidays"

SPARK_CONFIGS = {
    "spark.master": "spark://spark-master:7077",
    "spark.app.name": "holidays-etl",
    "spark.submit.deployMode": "client",

    "spark.driver.port": "5001",
    "spark.blockManager.port": "5002",
    "spark.driver.host": "airflow-webserver",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.executor.memory": "2g",
    "spark.driver.memory": "2g",

    "spark.pyspark.python": "python3",
    "spark.pyspark.driver.python": "python3",

    "spark.jars.packages": (
        "io.delta:delta-core_2.12:2.4.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.261"
    ),

    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",

    "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",

    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.dynamicAllocation.enabled": "false",
    "spark.sql.shuffle.partitions": "10",

    "spark.eventLog.enabled": "false",
}

DELTA_TABLE_CONFIGS = {
    "mergeSchema": "true",
    "checkpointLocation": f"{BASE_PATH}/_checkpoints",
    "overwriteSchema": "true"
}

REGIOES = {
    'norte': ['AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO'],
    'nordeste': ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE'],
    'centro_oeste': ['DF', 'GO', 'MT', 'MS'],
    'sudeste': ['ES', 'MG', 'RJ', 'SP'],
    'sul': ['PR', 'RS', 'SC']
}

# Processamento
BATCH_SIZE = 1000
MAX_RETRIES = 3
TIMEOUT = 300
