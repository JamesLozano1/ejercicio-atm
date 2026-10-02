# ==========================================
# 1. Importar librerías
# ==========================================

from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime


# ==========================================
# 2. Conexión con Kafka
# ==========================================

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


# ==========================================
# 3. Datos simulados
# ==========================================

# Cajeros automáticos que vamos a simular
atms = [
    "ATM-001",
    "ATM-002",
    "ATM-003",
    "ATM-004",
    "ATM-005"
]

# Tipos de transacciones que pueden realizar los cajeros
tipos_transaccion = [
    "RETIRO",
    "DEPOSITO",
    "CONSULTA_SALDO",
    "TRANSFERENCIA"
]


# ==========================================
# 4. Mensaje inicial
# ==========================================

print("🏧 Productor de transacciones ATM iniciado...")
print("Enviando transacciones a Kafka...\n")


# ==========================================
# 5. Generar transacciones continuamente
# ==========================================

contador = 1

while True:

    # Seleccionar un cajero aleatoriamente
    atm_id = random.choice(atms)

    # Seleccionar un tipo de transacción aleatoriamente
    tipo = random.choice(tipos_transaccion)

    # ==========================================
    # 6. Generar el monto de la transacción
    # ==========================================

    # Las consultas de saldo no mueven dinero
    if tipo == "CONSULTA_SALDO":
        monto = 0

    else:
        # Montos disponibles para las transacciones
        monto = random.choice([
            50000,
            100000,
            150000,
            200000,
            300000,
            500000,
            1000000
        ])

    # ==========================================
    # 7. Crear el evento de transacción
    # ==========================================

    transaccion = {
        "atm_id": atm_id,
        "transaction_id": f"TX-{contador:04d}",
        "transaction_type": tipo,
        "amount": monto,
        "currency": "COP",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    # ==========================================
    # 8. Enviar evento a Kafka
    # ==========================================

    producer.send(
        "transacciones-atm",
        value=transaccion
    )

    # Asegurar que el mensaje sea enviado
    producer.flush()

    # ==========================================
    # 9. Mostrar la transacción en pantalla
    # ==========================================

    print(
        f"🏧 {atm_id} | "
        f"{tipo} | "
        f"${monto:,} COP | "
        f"TX-{contador:04d}"
    )

    # ==========================================
    # 10. Preparar la siguiente transacción
    # ==========================================

    contador += 1

    # Esperar entre 1 y 3 segundos
    time.sleep(random.randint(1, 3))
