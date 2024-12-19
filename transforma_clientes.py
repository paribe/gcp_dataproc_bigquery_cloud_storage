from pyspark.sql import SparkSession
from pyspark.sql.functions import when

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("TransformaClientes").getOrCreate()

# Caminho do arquivo de entrada e saída no Cloud Storage
input_path = "gs://meu-bucket-clientes/input/cliente.csv"
output_path = "gs://meu-bucket-clientes/output/transformado/"

# Lê o arquivo CSV
df = spark.read.csv(input_path, header=True, sep=";")

# Adiciona a coluna "title" baseada no sexo
df_transformed = df.withColumn(
    "title", when(df["sexo"] == "M", "Mr").when(df["sexo"] == "F", "Ms")
)

# Grava os dados transformados no Cloud Storage como parquet
df_transformed.write.parquet(output_path, mode="overwrite")

spark.stop()
