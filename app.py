
from flask import Flask, render_template, request, jsonify
from kafka import KafkaProducer
from datetime import datetime
import json
import uuid

from pymongo import MongoClient
from pymongo.errors import PyMongoError

app = Flask(__name__)


# ==========================================
# 1. CONEXION CON MONGODB
# ==========================================

mongo_cliente = MongoClient(
    "mongodb://localhost:27017/",
    serverSelectionTimeoutMS=5000
)

base_datos = mongo_cliente["monitoreo_atm"]

coleccion_transacciones = base_datos["transacciones"]
coleccion_alertas = base_datos["alertas"]


# ==========================================
# 2. CONFIGURACION DE KAFKA
# ==========================================

KAFKA_SERVIDOR = "localhost:9092"
TOPICO = "transacciones-atm"

productor = KafkaProducer(
    bootstrap_servers=KAFKA_SERVIDOR,
    value_serializer=lambda dato: json.dumps(dato).encode("utf-8")
)


# ==========================================
# 3. PAGINA PRINCIPAL
# ==========================================

@app.route("/")
def inicio():
    return render_template("index.html")


# ==========================================
# 4. ENVIAR TRANSACCIONES A KAFKA
# ==========================================

@app.route("/api/transacciones", methods=["POST"])
def enviar_transaccion():
    datos = request.get_json(silent=True) or {}

    atm_id = str(datos.get("atm_id", "")).strip()
    tipo = str(datos.get("transaction_type", "")).strip()
    usuario = str(datos.get("usuario", "Usuario web")).strip()[:60]
    monto_recibido = datos.get("amount", 0)

    cajeros_validos = {
        "ATM-001", "ATM-002", "ATM-003",
        "ATM-004", "ATM-005"
    }

    tipos_validos = {
        "RETIRO", "DEPOSITO",
        "CONSULTA_SALDO", "TRANSFERENCIA"
    }

    if atm_id not in cajeros_validos:
        return jsonify({
            "error": "Selecciona un cajero valido."
        }), 400

    if tipo not in tipos_validos:
        return jsonify({
            "error": "Selecciona un tipo de transaccion valido."
        }), 400

    try:
        monto = float(monto_recibido)

        if not monto.is_integer():
            raise ValueError

        monto = int(monto)

    except (TypeError, ValueError):
        return jsonify({
            "error": "El monto debe ser un numero entero."
        }), 400

    if tipo == "CONSULTA_SALDO":
        monto = 0

    elif monto <= 0 or monto > 100_000_000:
        return jsonify({
            "error": (
                "El monto debe ser mayor que cero y "
                "no superar $100.000.000 COP."
            )
        }), 400

    transaccion = {
        "atm_id": atm_id,
        "transaction_id": f"TX-{uuid.uuid4().hex[:12].upper()}",
        "transaction_type": tipo,
        "amount": monto,
        "currency": "COP",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "dashboard_web",
        "usuario": usuario or "Usuario web"
    }

    try:
        resultado = productor.send(
            TOPICO,
            value=transaccion
        )

        resultado.get(timeout=10)
        productor.flush()

        return jsonify({
            "mensaje": "Transaccion enviada correctamente a Kafka.",
            "transaccion": transaccion
        }), 201

    except Exception:
        app.logger.exception("Error enviando transaccion a Kafka.")

        return jsonify({
            "error": (
                "No se pudo enviar la transaccion. "
                "Verifica que Kafka este funcionando."
            )
        }), 503


# ==========================================
# 5. CONSULTAR EL HISTORIAL EN MONGODB
# ==========================================

@app.route("/api/transacciones", methods=["GET"])
def consultar_transacciones():
    try:
        transacciones = list(
            coleccion_transacciones.find(
                {},
                {"_id": 0}
            ).sort(
                "fecha_procesamiento", -1
            ).limit(100)
        )

        return jsonify({
            "total": len(transacciones),
            "transacciones": transacciones
        }), 200

    except PyMongoError:
        app.logger.exception(
            "Error consultando transacciones en MongoDB."
        )

        return jsonify({
            "error": "No se pudo consultar el historial."
        }), 500


# ==========================================
# 6. CONSULTAR ALERTAS EN MONGODB
# ==========================================

@app.route("/api/alertas", methods=["GET"])
def consultar_alertas():
    try:
        alertas = list(
            coleccion_alertas.find(
                {},
                {"_id": 0}
            ).sort(
                "fecha_deteccion", -1
            ).limit(50)
        )

        return jsonify({
            "total": len(alertas),
            "alertas": alertas
        }), 200

    except PyMongoError:
        app.logger.exception(
            "Error consultando alertas en MongoDB."
        )

        return jsonify({
            "error": "No se pudieron consultar las alertas."
        }), 500


# ==========================================
# 7. INICIAR SERVIDOR FLASK
# ==========================================

if __name__ == "__main__":
    try:
        app.run(
            host="0.0.0.0",
            port=5000,
            debug=False
        )

    finally:
        productor.close()
        mongo_cliente.close()
