from pyspark.sql import SparkSession
from dags.conf.settings import SPARK_CONFIGS

def test_spark_connection():
    try:
        # Criar sessão Spark
        spark = SparkSession.builder
        
        for key, value in SPARK_CONFIGS.items():
            spark = spark.config(key, value)
        
        spark = spark.getOrCreate()
        
        # Criar um DataFrame de teste
        test_data = [("test", 1), ("test2", 2)]
        test_df = spark.createDataFrame(test_data, ["col1", "col2"])
        
        # Executar uma ação
        count = test_df.count()
        print(f"Conexão com Spark estabelecida com sucesso. Contagem de registros: {count}")
        
        spark.stop()
        return True
        
    except Exception as e:
        print(f"Erro ao conectar com Spark: {str(e)}")
        return False