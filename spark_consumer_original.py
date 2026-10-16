from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# ==========================================
# 1. Crear sesión de Spark
# ==========================================

spark = (
    SparkSession.builder
    .appName("MonitoreoATM")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ==========================================
# 2. Definir estructura de las transacciones
# ==========================================

schema = StructType([
    StructField("atm_id", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("currency", StringType(), True),
    StructField("timestamp", StringType(), True)
])


# ==========================================
# 3. Leer datos desde Kafka
# ==========================================

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "transacciones-atm")
    .option("startingOffsets", "latest")
    .load()
)


# ==========================================
# 4. Convertir JSON recibido desde Kafka
# ==========================================

datos = (
    df
    .selectExpr("CAST(value AS STRING) AS json")
    .select(
        from_json(col("json"), schema).alias("data")
    )
    .select("data.*")
)


# ==========================================
# 5. Procesar las transacciones
# ==========================================

resultado = (
    datos
    .groupBy("atm_id")
    .agg(
        count("*").alias("cantidad_transacciones"),
        sum("amount").alias("dinero_movido")
    )
)


# ==========================================
# 6. Mostrar resultados en consola
# ==========================================

consulta = (
    resultado
    .writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .option("numRows", 20)
    .start()
)


# ==========================================
# 7. Mantener Spark ejecutándose
# ==========================================

consulta.awaitTermination()
