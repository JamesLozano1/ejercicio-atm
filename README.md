# 🏧 Monitoreo de Redes de Cajeros Automáticos (ATMs)

Ejercicio práctico de **procesamiento de datos en tiempo real** con **Python, Flask, Apache Kafka, Apache Spark Structured Streaming y MongoDB**, todo apoyado en **Docker**.

Las transacciones de una red de cajeros automáticos se generan desde un **dashboard web** (o con un simulador automático), viajan por Kafka, Spark las procesa y las guarda en MongoDB, y el dashboard muestra el historial y las **alertas** de montos elevados.

---

## 📑 Tabla de contenido

1. [Objetivo y arquitectura](#-objetivo-y-arquitectura)
2. [Tecnologías](#-tecnologías)
3. [Requisitos previos](#-requisitos-previos)
4. [Descargar el proyecto](#-descargar-el-proyecto)
5. [Estructura del proyecto](#-estructura-del-proyecto)
6. [Instalación y ejecución paso a paso](#-instalación-y-ejecución-paso-a-paso)
7. [Resultado esperado](#-resultado-esperado)
8. [Cómo funciona](#-cómo-funciona)
9. [API del dashboard](#-api-del-dashboard)
10. [Detener y limpiar](#-detener-y-limpiar)
11. [Solución de problemas](#-solución-de-problemas)

---

## 🎯 Objetivo y arquitectura

Construir un flujo completo de procesamiento de eventos en tiempo real, desde que se origina la transacción hasta que se almacena y se visualiza:

```text
🌐 Dashboard web (app.py)          🏧 Productor automático (productor.py)
        │   localhost:9092                  │   localhost:9092
        └──────────────┬────────────────────┘
                       ▼
        📨 Apache Kafka  (tópico: transacciones-atm)
                       │   kafka:29092
                       ▼
        ⚡ Spark Structured Streaming (spark_consumer.py)
                       │   atm-mongodb:27017
                       ▼
        🍃 MongoDB  (base: monitoreo_atm)
              ├── transacciones   (todas las transacciones)
              └── alertas         (montos superiores a $800.000 COP)
                       │   localhost:27017
                       ▼
        🌐 Dashboard web: historial y alertas (se actualiza cada 5 s)
```

Tipos de operación:

* `RETIRO`
* `DEPOSITO`
* `CONSULTA_SALDO` (no mueve dinero, monto = 0)
* `TRANSFERENCIA`

Ejemplo de evento enviado desde el dashboard:

```json
{
  "atm_id": "ATM-003",
  "transaction_id": "TX-9F2A41C7B0D3",
  "transaction_type": "TRANSFERENCIA",
  "amount": 500000,
  "currency": "COP",
  "timestamp": "2026-10-15 21:45:30",
  "source": "dashboard_web",
  "usuario": "Estudiante 1"
}
```

> Los eventos del simulador (`productor.py`) tienen los mismos campos, salvo `source` y `usuario`, y su `transaction_id` es secuencial (`TX-0001`, `TX-0002`, ...).

---

## 🛠️ Tecnologías

| Tecnología                 | Uso                                              |
| -------------------------- | ------------------------------------------------ |
| Python 3.12                | Dashboard, API y simulador de transacciones      |
| Flask                      | Servidor web, dashboard y API REST               |
| kafka-python 2.2.15        | Comunicación entre Python y Kafka                |
| Apache Kafka               | Mensajería y transmisión de eventos              |
| Apache Spark 3.5.7         | Procesamiento de datos                           |
| Spark Structured Streaming | Procesamiento continuo en micro-batches          |
| MongoDB 7                  | Almacenamiento persistente de transacciones y alertas |
| PyMongo                    | Acceso a MongoDB desde Flask (4.19.0) y Spark (4.10.1) |
| Docker / Docker Compose    | Ejecución de Kafka, MongoDB y Spark              |
| PowerShell                 | Ejecución de comandos en Windows                 |

---

## ✅ Requisitos previos

Instalar antes de empezar:

| Programa | Para qué | Descarga |
| -------- | -------- | -------- |
| **Docker Desktop** | Correr Kafka, MongoDB y Spark en contenedores | https://www.docker.com/products/docker-desktop/ |
| **Python 3.12** | Ejecutar el dashboard y el productor | https://www.python.org/downloads/ |
| **Git** (opcional) | Clonar el repositorio | https://git-scm.com/downloads |

Notas importantes:

* Al instalar Python en Windows, marca la casilla **"Add Python to PATH"**.
* Después de instalar Docker Desktop, **ábrelo y espera a que diga "Engine running"** antes de continuar.
* Se necesita **conexión a internet** la primera vez (se descargan imágenes de Docker y paquetes de Spark).
* Recomendado: al menos **4 GB de RAM** libres para Docker.
* Puertos que deben estar libres: **9092** (Kafka), **27017** (MongoDB) y **5000** (dashboard).

Verifica que todo quedó instalado (en PowerShell):

```powershell
docker --version
docker compose version
python --version
```

---

## 📥 Descargar el proyecto

### Opción A: con Git

```powershell
cd "$HOME\Desktop"
git clone <URL-DEL-REPOSITORIO> ejercicio-atm
cd ejercicio-atm
```

> Reemplaza `<URL-DEL-REPOSITORIO>` por la URL de tu repositorio.

### Opción B: descargando el ZIP

1. Descarga el ZIP del proyecto y extráelo en el Escritorio.
2. Asegúrate de que la carpeta se llame **`ejercicio-atm`** (esto es importante, ver nota más abajo).
3. Abre PowerShell dentro de la carpeta:

```powershell
cd "$HOME\Desktop\ejercicio-atm"
```

> ⚠️ **Sobre el nombre de la carpeta:** Docker Compose nombra la red como `<nombre-de-la-carpeta>_atm-network`. Con la carpeta `ejercicio-atm`, la red será `ejercicio-atm_atm-network`, que es la que usa el comando de Spark en el paso 5. Si cambias el nombre de la carpeta, ajusta ese comando (ver [solución de problemas](#-solución-de-problemas)).

---

## 📁 Estructura del proyecto

```text
ejercicio-atm/
│
├── docker-compose.yml          # Contenedores de Kafka y MongoDB, y su red
├── Dockerfile.spark            # Imagen de Spark con PyMongo instalado
├── requirements.txt            # Dependencias de Python (Flask, kafka-python, pymongo)
├── app.py                      # Servidor Flask: dashboard y API REST
├── productor.py                # Simulador automático de transacciones ATM
├── spark_consumer.py           # Spark Streaming: guarda transacciones y genera alertas
├── templates/
│   └── index.html              # Página del dashboard
├── static/
│   ├── css/style.css           # Estilos del dashboard
│   └── js/dashboard.js         # Lógica del dashboard (envío y actualización cada 5 s)
├── spark_consumer_original.py  # Versiones anteriores del consumidor (solo referencia)
├── spark_consumer_antes_transacciones.py
└── .ivy2/                      # Caché de paquetes de Spark (se crea en el paso 4)
```

### `docker-compose.yml`

Levanta dos servicios en la misma red (`atm-network`):

* **`atm-kafka`**: Apache Kafka en modo KRaft (sin Zookeeper) con **dos listeners**:

  ```text
  EXTERNAL → localhost:9092   (para Python en Windows)
  INTERNAL → kafka:29092      (para Spark dentro de Docker)
  ```

  Los tópicos nuevos se crean por defecto con **3 particiones** y **1 réplica**.

* **`atm-mongodb`**: MongoDB 7 expuesto en `localhost:27017`, con el volumen `mongodb-data` para que los datos **se conserven** al apagar y volver a encender los contenedores.

### `Dockerfile.spark`

Parte de `apache/spark:3.5.7` e instala `pymongo`, necesario porque el consumidor escribe en MongoDB. La imagen oficial de Spark no lo trae, por eso hay que construir esta imagen (paso 4).

### `app.py`

Servidor Flask (puerto `5000`) que:

* Sirve el dashboard en `/`.
* Valida y envía las transacciones a Kafka (`POST /api/transacciones`).
* Consulta el historial y las alertas guardados en MongoDB (`GET /api/transacciones` y `GET /api/alertas`).

Validaciones: el cajero debe ser `ATM-001` … `ATM-005`, el tipo debe ser uno de los cuatro válidos, y el monto debe ser un entero mayor que 0 y de máximo `$100.000.000` COP (en `CONSULTA_SALDO` el monto siempre se guarda como 0).

### `productor.py`

Simula 5 cajeros (`ATM-001` … `ATM-005`), genera una transacción aleatoria cada 1 a 3 segundos y la envía al tópico `transacciones-atm`. Los montos posibles son `50.000`, `100.000`, `150.000`, `200.000`, `300.000`, `500.000` y `1.000.000` COP; este último supera el umbral y dispara alertas. **Es opcional**: sirve para generar carga automática sin usar el formulario.

### `spark_consumer.py`

Lee el tópico con Spark Structured Streaming y, para cada micro-batch (`foreachBatch`):

* Guarda **todas** las transacciones en la colección `transacciones` de MongoDB.
* Crea una **alerta** (`MONTO_ELEVADO`, severidad `ALTA`) en la colección `alertas` cuando el monto supera **`$800.000` COP** y la operación no es `CONSULTA_SALDO`.
* Usa `transaction_id` y `alert_id` como llave con `upsert`, por lo que si un lote se reintenta **no se generan duplicados**.

### Dashboard (`templates/` y `static/`)

* **Enviar transacción:** formulario con usuario, cajero, tipo y monto.
* **Indicadores de la sesión:** enviadas, confirmadas y con error.
* **Últimos envíos:** actividad de la sesión actual del navegador.
* **Transacciones almacenadas:** hasta 100 registros recientes de MongoDB.
* **Alertas detectadas:** hasta 50 alertas recientes de MongoDB.

El historial y las alertas se refrescan automáticamente cada 5 segundos.

---

# 🚀 Instalación y ejecución paso a paso

> Necesitarás **tres ventanas de PowerShell** abiertas en la carpeta del proyecto (cuatro si también quieres usar el productor automático):
> una para Kafka/MongoDB y comandos, una para Spark y una para el dashboard.

## 1. Instalar las dependencias de Python

Se recomienda usar un entorno virtual:

```powershell
cd "$HOME\Desktop\ejercicio-atm"
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

> Si PowerShell bloquea la activación del entorno, ejecuta una vez:
> `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`
> y vuelve a activar el entorno.

## 2. Levantar Kafka y MongoDB

Con Docker Desktop abierto:

```powershell
docker compose up -d
```

La primera vez descargará las imágenes `apache/kafka` y `mongo:7`. Verifica que estén corriendo:

```powershell
docker ps
```

Deben aparecer los contenedores `atm-kafka` y `atm-mongodb` con estado `Up`.

También puedes confirmar el nombre de la red:

```powershell
docker network ls
```

Debe aparecer `ejercicio-atm_atm-network`.

## 3. Verificar / crear el tópico

Espera unos 10 a 15 segundos a que Kafka termine de arrancar y lista los tópicos:

```powershell
docker exec -it atm-kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

Si **no aparece** `transacciones-atm`, créalo manualmente (también se crea automáticamente cuando llega el primer mensaje):

```powershell
docker exec -it atm-kafka /opt/kafka/bin/kafka-topics.sh --create --topic transacciones-atm --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

## 4. Preparar Spark (solo la primera vez)

Crea la carpeta de caché de paquetes y construye la imagen de Spark con PyMongo:

```powershell
mkdir .ivy2
docker build -f Dockerfile.spark -t spark-atm-mongo .
```

> `spark-atm-mongo` es el nombre que se le da a la imagen; puedes usar otro, pero debes usar el mismo en el paso siguiente.

## 5. Ejecutar Spark Streaming (Terminal 2)

Abre una **nueva PowerShell**, entra a la carpeta del proyecto y ejecuta:

```powershell
cd "$HOME\Desktop\ejercicio-atm"

docker run --rm -it `
  --network ejercicio-atm_atm-network `
  -v "${PWD}:/app" `
  -v "${PWD}\.ivy2:/tmp/.ivy2" `
  -e HOME=/tmp `
  spark-atm-mongo `
  /opt/spark/bin/spark-submit `
  --conf spark.jars.ivy=/tmp/.ivy2 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 `
  /app/spark_consumer.py
```

* La **primera vez** tarda unos minutos porque descarga el conector de Kafka (queda guardado en `.ivy2`).
* Cuando esté listo verás:

  ```text
  Spark esta monitoreando todas las transacciones ATM.
  Umbral de alerta: $800,000 COP
  ```

* El consumidor usa `startingOffsets = latest`, por lo que **solo procesa las transacciones que lleguen después de que Spark arranque**. Deja esta terminal abierta antes de enviar datos.

## 6. Iniciar el dashboard (Terminal 3)

Abre otra **PowerShell**:

```powershell
cd "$HOME\Desktop\ejercicio-atm"
.\venv\Scripts\Activate.ps1
python app.py
```

Abre en el navegador: **http://localhost:5000**

Desde ahí puedes llenar el formulario y pulsar **"Enviar a Kafka"**. Para probar una alerta, envía un retiro, depósito o transferencia **mayor a $800.000** (por ejemplo, `1000000`).

> Kafka debe estar encendido antes de iniciar `app.py`, porque el productor de Kafka se crea al arrancar la aplicación.

## 7. (Opcional) Ejecutar el productor automático (Terminal 4)

Si quieres generar tráfico continuo sin usar el formulario:

```powershell
cd "$HOME\Desktop\ejercicio-atm"
.\venv\Scripts\Activate.ps1
python productor.py
```

Salida esperada:

```text
🏧 Productor de transacciones ATM iniciado...
Enviando transacciones a Kafka...

🏧 ATM-005 | TRANSFERENCIA | $100,000 COP | TX-0001
🏧 ATM-003 | RETIRO | $300,000 COP | TX-0002
🏧 ATM-002 | DEPOSITO | $1,000,000 COP | TX-0003
```

Para detenerlo: `Ctrl + C`.

---

# 📊 Resultado esperado

**En la terminal de Spark** aparecerá un mensaje por cada transacción guardada, las alertas y un resumen de cada lote:

```text
TRANSACCION GUARDADA: ATM-003 - RETIRO - $300,000 COP
TRANSACCION GUARDADA: ATM-002 - DEPOSITO - $1,000,000 COP
ALERTA: ATM-002 - $1,000,000 COP
LOTE 4 FINALIZADO: 2 transacciones procesadas; 1 alertas detectadas.
```

**En el dashboard** (`http://localhost:5000`):

* La tabla **Transacciones almacenadas** muestra las transacciones guardadas en MongoDB, ordenadas de la más reciente a la más antigua.
* La tabla **Alertas detectadas** muestra las transacciones que superaron `$800.000` COP, con severidad `ALTA`.
* Los datos pueden tardar unos segundos en aparecer (Spark procesa por micro-batches y el dashboard consulta cada 5 s).

**Verificación directa en MongoDB** (opcional):

```powershell
docker exec -it atm-mongodb mongosh monitoreo_atm --eval "db.transacciones.countDocuments(); db.alertas.countDocuments()"
```

> Los datos del simulador son aleatorios, así que los resultados cambian en cada ejecución.

---

# 🔄 Cómo funciona

1. **Generación:** el dashboard (`app.py`) valida el formulario y arma la transacción; el simulador (`productor.py`) crea transacciones aleatorias.
2. **Envío:** ambos serializan el evento a JSON y lo publican en el tópico `transacciones-atm` mediante `localhost:9092`. El dashboard espera la confirmación de Kafka antes de responder `201`.
3. **Recepción:** Kafka almacena los eventos y los deja disponibles para los consumidores.
4. **Procesamiento:** Spark se conecta por la red interna de Docker (`kafka:29092`), lee los eventos y convierte el JSON a columnas usando un esquema definido.
5. **Almacenamiento:** en cada micro-batch, `procesar_lote` guarda cada transacción en MongoDB (`atm-mongodb:27017`) y, si supera el umbral, registra una alerta.
6. **Visualización:** el dashboard consulta MongoDB (`localhost:27017`) a través de la API y actualiza las tablas cada 5 segundos.

### Conceptos aplicados

* **Apache Kafka:** broker de mensajería con 3 particiones y 1 réplica; dos listeners (`EXTERNAL` e `INTERNAL`) para que Windows y los contenedores puedan conectarse.
* **Spark Structured Streaming:** trata el flujo como una tabla que crece continuamente y procesa los datos en micro-batches.
* **`foreachBatch`:** permite aplicar lógica propia a cada micro-batch (aquí, escribir en MongoDB y evaluar las reglas de alerta).
* **Idempotencia:** el `upsert` con `$setOnInsert` sobre `transaction_id` y `alert_id` evita registros duplicados si un lote se reprocesa.
* **Persistencia:** MongoDB guarda los datos en un volumen de Docker, de modo que sobreviven a `docker compose down`.
* **Arquitectura por capas:** ingesta (Flask/productor) → mensajería (Kafka) → procesamiento (Spark) → almacenamiento (MongoDB) → visualización (dashboard).

---

# 🔌 API del dashboard

| Método | Ruta                | Descripción                                                                 |
| ------ | ------------------- | --------------------------------------------------------------------------- |
| `GET`  | `/`                 | Página del dashboard.                                                       |
| `POST` | `/api/transacciones`| Valida y envía una transacción a Kafka. Responde `201`, `400` (datos inválidos) o `503` (Kafka no disponible). |
| `GET`  | `/api/transacciones`| Últimas 100 transacciones guardadas en MongoDB.                             |
| `GET`  | `/api/alertas`      | Últimas 50 alertas guardadas en MongoDB.                                    |

Ejemplo de envío desde PowerShell:

```powershell
Invoke-RestMethod -Uri http://localhost:5000/api/transacciones `
  -Method Post -ContentType "application/json" `
  -Body '{"atm_id":"ATM-001","transaction_type":"RETIRO","amount":1000000,"usuario":"Prueba"}'
```

---

# 🧹 Detener y limpiar

1. Detener el dashboard, Spark y el productor con `Ctrl + C` en cada terminal.
2. Apagar Kafka y MongoDB (los datos de MongoDB **se conservan**):

```powershell
docker compose down
```

3. (Opcional) Apagar y **borrar también los datos de MongoDB**:

```powershell
docker compose down -v
```

4. (Opcional) Borrar la caché de paquetes de Spark:

```powershell
Remove-Item -Recurse -Force .ivy2
```

Para volver a ejecutar todo desde cero, repite desde el [paso 2](#2-levantar-kafka-y-mongodb).

---

# 🩺 Solución de problemas

| Problema | Causa probable | Solución |
| -------- | -------------- | -------- |
| `error during connect` / Docker no responde | Docker Desktop no está abierto | Abre Docker Desktop y espera a que diga "Engine running". |
| `NoBrokersAvailable` al iniciar `app.py` o `productor.py` | Kafka aún no arrancó o está apagado | Revisa `docker ps`, espera unos segundos y vuelve a intentar. Mira los logs con `docker logs atm-kafka`. |
| `ModuleNotFoundError: No module named 'kafka'` o `'flask'` | Faltan las dependencias o el entorno no está activo | Activa el entorno (`.\venv\Scripts\Activate.ps1`) y ejecuta `pip install -r requirements.txt`. |
| `No module named 'kafka.vendor.six.moves'` | Versión antigua de `kafka-python` incompatible con Python 3.12 | `pip install -r requirements.txt` (usa `kafka-python==2.2.15`). |
| `ModuleNotFoundError: No module named 'pymongo'` en Spark | Se ejecutó la imagen `apache/spark:3.5.7` en lugar de la imagen propia | Construye la imagen (`docker build -f Dockerfile.spark -t spark-atm-mongo .`) y úsala en el comando del paso 5. |
| `network ejercicio-atm_atm-network not found` | La carpeta tiene otro nombre o los contenedores no están levantados | Ejecuta `docker network ls`, copia el nombre real de la red terminada en `_atm-network` y úsalo en el comando de Spark. |
| Spark no puede resolver `kafka:29092` o `atm-mongodb` | Spark no está en la misma red de Docker | Verifica que el comando incluya `--network ejercicio-atm_atm-network`. |
| `ERROR procesando lote ...` con error de MongoDB en Spark | MongoDB está apagado o aún no arranca | Revisa `docker ps` (debe estar `atm-mongodb`) y reinicia con `docker compose up -d`. |
| El dashboard dice "No se pudo consultar el historial" | MongoDB apagado o puerto `27017` no disponible | Verifica que `atm-mongodb` esté `Up` y que nada más use el puerto 27017. |
| El dashboard muestra "No se pudo enviar la transacción" | Kafka no está funcionando | Revisa `docker ps` y `docker logs atm-kafka`; reinicia `app.py` si Kafka estuvo apagado. |
| Envío exitoso pero el historial no muestra datos | Spark no está corriendo, o se inició después del envío (offset `latest`) | Deja Spark corriendo **antes** de enviar transacciones y espera unos segundos. |
| No aparecen alertas | El monto no supera `$800.000` o es una consulta de saldo | Envía un retiro, depósito o transferencia mayor a `800000`. |
| Falla la descarga de `--packages` | Sin internet o problema de permisos en `.ivy2` | Revisa la conexión, confirma que la carpeta `.ivy2` existe y vuelve a ejecutar. |
| `${PWD}` no se reconoce | Estás usando CMD en lugar de PowerShell | Usa PowerShell, o reemplaza `${PWD}` por la ruta completa del proyecto. |
| Puerto `9092`, `27017` o `5000` ocupado | Otro servicio usa ese puerto | Cierra el otro servicio o cambia el puerto en `docker-compose.yml` / `app.py` (y en el código que lo usa). |

---

# 👨‍💻 Autor

**Ejercicio académico: Monitoreo de Redes de Cajeros Automáticos (ATMs)**

Tecnologías: **Python + Flask + Apache Kafka + Apache Spark + MongoDB + Docker**