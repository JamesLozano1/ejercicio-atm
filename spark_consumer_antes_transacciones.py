
import json
from datetime import datetime, timezone

from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)


# ==========================================
# 1. Configuración
# ==========================================

KAFKA_SERVIDOR = "kafka:29092"
TOPICO = "transacciones-atm"
MONGODB_URI = "mongodb://atm-mongodb:27017/"
MONTO_LIMITE = 800_000


# ==========================================
# 2. Crear sesión de Spark
# ==========================================

spark = (
    SparkSession.builder
    .appName("MonitoreoATMAlertas")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ==========================================
# 3. Estructura de las transacciones
# ==========================================

schema = StructType([
    StructField("atm_id", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("currency", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("source", StringType(), True),
    StructField("usuario", StringType(), True),
])


# ==========================================
# 4. Leer transacciones desde Kafka
# ==========================================

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVIDOR)
    .option("subscribe", TOPICO)
    .option("startingOffsets", "latest")
    .load()
)


# ==========================================
# 5. Convertir el JSON
# ==========================================

datos = (
    df.selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), schema).alias("data"))
    .select("data.*")
)


# ==========================================
# 6. Detectar transacciones de alto monto
# ==========================================

alertas = (
    datos
    .filter(
        (col("amount") > MONTO_LIMITE)
        & (col("transaction_type") != "CONSULTA_SALDO")
    )
)


# ==========================================
# 7. Guardar alertas en MongoDB
# ==========================================

def guardar_alertas_en_mongodb(lote, id_lote):
    if lote.isEmpty():
        return

    cliente = MongoClient(
        MONGODB_URI,
        serverSelectionTimeoutMS=5000
    )

    try:
        coleccion = cliente["monitoreo_atm"]["alertas"]

        for fila in lote.toLocalIterator():
            transaccion = fila.asDict(recursive=True)
            transaction_id = transaccion.get("transaction_id")

            alerta = {
                "alert_id": f"MONTO-{transaction_id}",
                "tipo_alerta": "MONTO_ELEVADO",
                "severidad": "ALTA",
                "descripcion": (
                    f"Transacción superior a "
                    f"${MONTO_LIMITE:,.0f} COP"
                ),
                "atm_id": transaccion.get("atm_id"),
                "transaction_id": transaction_id,
                "transaction_type": transaccion.get("transaction_type"),
                "amount": transaccion.get("amount"),
                "currency": transaccion.get("currency", "COP"),
                "timestamp_transaccion": transaccion.get("timestamp"),
                "usuario": transaccion.get("usuario"),
                "fecha_deteccion": datetime.now(timezone.utc).isoformat(),
                "lote_spark": id_lote,
            }

            # Evitar duplicar alertas si Spark reintenta un lote.
            coleccion.update_one(
                {"alert_id": alerta["alert_id"]},
                {"$setOnInsert": alerta},
                upsert=True
            )

            print(
                f"ALERTA: {alerta['atm_id']} - "
                f"${alerta['amount']:,} COP"
            )

    finally:
        cliente.close()


# ==========================================
# 8. Ejecutar el procesamiento continuo
# ==========================================

consulta = (
    alertas.writeStream
    .foreachBatch(guardar_alertas_en_mongodb)
    .option("checkpointLocation", "/tmp/checkpoint-alertas-atm")
    .start()
)

print("Spark está monitoreando transacciones de alto monto.")
consulta.awaitTermination()
