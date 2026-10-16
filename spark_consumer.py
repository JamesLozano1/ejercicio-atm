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
# 1. CONFIGURACION
# ==========================================

KAFKA_SERVIDOR = "kafka:29092"
TOPICO = "transacciones-atm"

MONGODB_URI = "mongodb://atm-mongodb:27017/"

BASE_DATOS = "monitoreo_atm"
COLECCION_TRANSACCIONES = "transacciones"
COLECCION_ALERTAS = "alertas"

MONTO_LIMITE = 800_000


# ==========================================
# 2. CREAR SESION DE SPARK
# ==========================================

spark = (
    SparkSession.builder
    .appName("MonitoreoATMCompleto")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ==========================================
# 3. ESTRUCTURA DE LOS MENSAJES
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
# 4. LEER TODAS LAS TRANSACCIONES DE KAFKA
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
# 5. CONVERTIR LOS MENSAJES JSON
# ==========================================

datos = (
    df.selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), schema).alias("data"))
    .select("data.*")
)


# ==========================================
# 6. GUARDAR TODAS LAS TRANSACCIONES
#    Y GENERAR ALERTAS CUANDO CORRESPONDA
# ==========================================

def procesar_lote(lote, id_lote):
    cliente = None

    try:
        cliente = MongoClient(
            MONGODB_URI,
            serverSelectionTimeoutMS=5000
        )

        base_datos = cliente[BASE_DATOS]

        coleccion_transacciones = base_datos[
            COLECCION_TRANSACCIONES
        ]

        coleccion_alertas = base_datos[
            COLECCION_ALERTAS
        ]

        total_procesadas = 0
        total_alertas = 0

        for fila in lote.toLocalIterator():
            transaccion = fila.asDict(recursive=True)

            transaction_id = transaccion.get("transaction_id")

            # Sin identificador no podemos evitar duplicados.
            if not transaction_id:
                print(
                    "AVISO: transaccion ignorada porque "
                    "no tiene transaction_id."
                )
                continue

            tipo = transaccion.get("transaction_type") or ""
            monto = transaccion.get("amount")
            monto = monto if monto is not None else 0

            moneda = transaccion.get("currency") or "COP"

            # Documento de la transaccion.
            documento = {
                "transaction_id": transaction_id,
                "atm_id": transaccion.get("atm_id"),
                "transaction_type": tipo,
                "amount": monto,
                "currency": moneda,
                "timestamp": transaccion.get("timestamp"),
                "source": transaccion.get("source"),
                "usuario": transaccion.get("usuario"),
                "fecha_procesamiento": (
                    datetime.now(timezone.utc).isoformat()
                ),
            }

            # Guardar TODAS las transacciones.
            # El identificador evita duplicados al reintentar lotes.
            coleccion_transacciones.update_one(
                {"transaction_id": transaction_id},
                {"$setOnInsert": documento},
                upsert=True
            )

            total_procesadas += 1

            print(
                f"TRANSACCION GUARDADA: "
                f"{documento['atm_id']} - "
                f"{tipo} - ${monto:,} {moneda}"
            )

            # Crear una alerta solo si se supera el umbral.
            if (
                monto > MONTO_LIMITE
                and tipo != "CONSULTA_SALDO"
            ):
                alerta = {
                    "alert_id": f"MONTO-{transaction_id}",
                    "tipo_alerta": "MONTO_ELEVADO",
                    "severidad": "ALTA",
                    "descripcion": (
                        f"Transaccion superior a "
                        f"${MONTO_LIMITE:,} COP"
                    ),
                    "atm_id": transaccion.get("atm_id"),
                    "transaction_id": transaction_id,
                    "transaction_type": tipo,
                    "amount": monto,
                    "currency": moneda,
                    "timestamp_transaccion": (
                        transaccion.get("timestamp")
                    ),
                    "usuario": transaccion.get("usuario"),
                    "fecha_deteccion": (
                        datetime.now(timezone.utc).isoformat()
                    ),
                    "lote_spark": id_lote,
                }

                # Guardar la alerta sin duplicarla.
                coleccion_alertas.update_one(
                    {"alert_id": alerta["alert_id"]},
                    {"$setOnInsert": alerta},
                    upsert=True
                )

                total_alertas += 1

                print(
                    f"ALERTA: {alerta['atm_id']} - "
                    f"${monto:,} {moneda}"
                )

        print(
            f"LOTE {id_lote} FINALIZADO: "
            f"{total_procesadas} transacciones procesadas; "
            f"{total_alertas} alertas detectadas."
        )

    except Exception as error:
        print(f"ERROR procesando lote {id_lote}: {error}")
        raise

    finally:
        if cliente is not None:
            cliente.close()


# ==========================================
# 7. EJECUTAR EL PROCESAMIENTO CONTINUO
# ==========================================

consulta = (
    datos.writeStream
    .foreachBatch(procesar_lote)
    .option(
        "checkpointLocation",
        "/tmp/checkpoint-alertas-atm"
    )
    .start()
)

print("Spark esta monitoreando todas las transacciones ATM.")
print(f"Umbral de alerta: ${MONTO_LIMITE:,} COP")

consulta.awaitTermination()
