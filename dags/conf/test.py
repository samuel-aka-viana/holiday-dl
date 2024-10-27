from pyspark.sql import SparkSession

# Configuração do Spark
spark = SparkSession.builder \
    .appName("MinIO Example") \
    .config("spark.hadoop.fs.s3a.access.key", "DUMMYIDEXAMPLE") \
    .config("spark.hadoop.fs.s3a.secret.key", "DUMMYEXAMPLEKEY") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Exemplo de dados
data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
columns = ["Name", "Age"]

# Criando um DataFrame
df = spark.createDataFrame(data, columns)

# Escrevendo no MinIO
df.write.csv("s3a://datalake/people.csv", header=True)

# Lendo do MinIO
df_read = spark.read.csv("s3a://datalake/people.csv", header=True)
df_read.show()

# Finalizando a sessão Spark
spark.stop()
