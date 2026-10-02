from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime


# Conexión con Kafka
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


# Datos simulados
atms = ["ATM-001", "ATM-002", "ATM-003", "ATM-004", "ATM-005"]

tipos_transaccion = [
    "RETIRO",
    "DEPOSITO",
    "CONSULTA_SALDO",
    "TRANSFERENCIA"
]


print("🏧 Productor de transacciones ATM iniciado...")
print("Enviando transacciones a Kafka...\n")


contador = 1

while True:

    atm_id = random.choice(atms)
    tipo = random.choice(tipos_transaccion)

    # Las consultas de saldo no mueven dinero
    if tipo == "CONSULTA_SALDO":
        monto = 0

    else:
        monto = random.choice([
            50000,
            100000,
            150000,
            200000,
            300000,
            500000,
            1000000
        ])

    transaccion = {
        "atm_id": atm_id,
        "transaction_id": f"TX-{contador:04d}",
        "transaction_type": tipo,
        "amount": monto,
        "currency": "COP",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    # Enviar evento a Kafka
    producer.send(
        "transacciones-atm",
        value=transaccion
    )

    producer.flush()

    print(
        f"🏧 {atm_id} | "
        f"{tipo} | "
        f"${monto:,} COP | "
        f"TX-{contador:04d}"
    )

    contador += 1

    # Esperar entre 1 y 3 segundos
    time.sleep(random.randint(1, 3))
