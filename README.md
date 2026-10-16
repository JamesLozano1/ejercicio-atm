# 🏧 Monitoreo de Redes de Cajeros Automáticos (ATMs)

Ejercicio práctico de **procesamiento de datos en tiempo real** con **Python, Flask, Apache Kafka, Apache Spark Structured Streaming y MongoDB**, todo apoyado en **Docker**.

Las transacciones de una red de cajeros automáticos se generan desde un **dashboard web** (de forma manual o con su simulador automático) o con un script productor por consola, viajan por Kafka, Spark las procesa y las guarda en MongoDB, y el dashboard muestra el historial y las **alertas** de montos elevados.

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
10. [Respaldo de MongoDB (copiar los datos a otro computador)](#-respaldo-de-mongodb)
11. [Detener y limpiar](#-detener-y-limpiar)
12. [Solución de problemas](#-solución-de-problemas)

---

## 🎯 Objetivo y arquitectura

Construir un flujo completo de procesamiento de eventos en tiempo real, desde que se origina la transacción hasta que se almacena y se visualiza:

```text
🌐 Dashboard web (app.py)          🏧 Productor por consola (productor.py)
   envío manual + simulador             (opcional)
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

> Las transacciones del simulador automático del dashboard usan este mismo formato, con el nombre de usuario que escribas. Las de `productor.py` tienen los mismos campos, salvo `source` y `usuario`, y su `transaction_id` es secuencial (`TX-0001`, `TX-0002`, ...).

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
git clone https://github.com/JamesLozano1/ejercicio-atm.git ejercicio-atm
cd ejercicio-atm
```

> El repositorio ya incluye el respaldo de la base de datos (`backups/monitoreo_atm.archive`), así que al clonarlo tendrás también los datos. Para cargarlos en MongoDB sigue el [paso 2.1](#21-restaurar-los-datos-de-mongodb-respaldo-incluido).

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
├── productor.py                # Simulador de transacciones por consola (opcional)
├── spark_consumer.py           # Spark Streaming: guarda transacciones y genera alertas
├── backups/
│   └── monitoreo_atm.archive   # Respaldo comprimido de la base MongoDB `monitoreo_atm` (transacciones y alertas)
├── templates/
│   └── index.html              # Página del dashboard
├── static/
│   ├── css/style.css           # Estilos del dashboard
│   └── js/dashboard.js         # Lógica del dashboard (envío manual, simulador automático, actualización cada 5 s)
├── spark_consumer_original.py  # Versiones anteriores del consumidor (solo referencia)
├── spark_consumer_antes_transacciones.py
└── .ivy2/                      # Caché de paquetes de Spark (se crea en el paso 4; no es necesario subirla a Git)
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

Simula 5 cajeros (`ATM-001` … `ATM-005`), genera una transacción aleatoria cada 1 a 3 segundos y la envía al tópico `transacciones-atm`. Los montos posibles son `50.000`, `100.000`, `150.000`, `200.000`, `300.000`, `500.000` y `1.000.000` COP; este último supera el umbral y dispara alertas. **Es opcional**: genera carga desde la consola; el dashboard ya incluye su propio simulador automático.

### `spark_consumer.py`

Lee el tópico con Spark Structured Streaming y, para cada micro-batch (`foreachBatch`):

* Guarda **todas** las transacciones en la colección `transacciones` de MongoDB.
* Crea una **alerta** (`MONTO_ELEVADO`, severidad `ALTA`) en la colección `alertas` cuando el monto supera **`$800.000` COP** y la operación no es `CONSULTA_SALDO`.
* Usa `transaction_id` y `alert_id` como llave con `upsert`, por lo que si un lote se reintenta **no se generan duplicados**.

### Dashboard (`templates/` y `static/`)

Interfaz web en `http://localhost:5000` con estas secciones:

* **Indicadores generales:** transacciones enviadas, confirmadas y con error (contadores de la sesión del navegador; se reinician al recargar la página) y total de alertas (cuenta las alertas que devuelve la API, máximo 50).
* **Nuevo evento (envío manual):** formulario con usuario (obligatorio), cajero, tipo y monto. Con `CONSULTA_SALDO` el monto se bloquea en `0`.
* **Simulador automático:** escribes el nombre del usuario que generará los eventos y pulsas **Iniciar envío automático**. El navegador envía una transacción aleatoria de alguno de los cinco cajeros **aproximadamente cada segundo**, hasta que pulses **Detener envío automático** (se completa la solicitud en curso). Cerca del 30 % de las operaciones con dinero supera los `$800.000` COP (y por tanto genera alerta); el resto es de `$10.000` a `$790.000`, en múltiplos de `$10.000`. Mientras el simulador corre, el envío manual queda deshabilitado, y solo funciona mientras la pestaña del dashboard esté abierta.
* **Últimos envíos:** las 10 transacciones más recientes, leídas desde MongoDB.
* **Historial de transacciones:** hasta 100 registros recientes de MongoDB.
* **Alertas de seguridad:** hasta 50 alertas recientes de MongoDB, con ID de transacción, cajero, tipo, monto y motivo.

El historial, las alertas y los últimos envíos se refrescan automáticamente cada 5 segundos. Como esas tablas se leen de MongoDB, una transacción aparece solo después de que Spark la haya procesado.

---

# 🚀 Instalación y ejecución paso a paso

> Necesitarás **tres ventanas de PowerShell** abiertas en la carpeta del proyecto (cuatro si también quieres usar el productor por consola):
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

## 2.1 Restaurar los datos de MongoDB (respaldo incluido)

Si descargaste el proyecto de GitHub, la carpeta `backups/` trae una copia de la base `monitoreo_atm` con las transacciones y alertas que se generaron en el computador original. Para tenerlas en este equipo, con `atm-mongodb` en estado `Up` (paso 2), ejecuta desde la carpeta del proyecto:

```powershell
docker cp .\backups\monitoreo_atm.archive atm-mongodb:/tmp/monitoreo_atm.archive

docker exec atm-mongodb mongorestore --archive=/tmp/monitoreo_atm.archive --gzip --drop
```

Comprueba que se cargaron:

```powershell
docker exec -it atm-mongodb mongosh monitoreo_atm --quiet --eval "print('transacciones: ' + db.transacciones.countDocuments()); print('alertas: ' + db.alertas.countDocuments())"
```

Con el respaldo actual deberías ver aproximadamente **7.800 transacciones** y **1.800 alertas**. El dashboard (paso 6) mostrará estos datos de inmediato, **sin necesidad de tener Spark corriendo**; Spark solo hace falta para procesar transacciones nuevas.

> ⚠️ `--drop` borra las colecciones `transacciones` y `alertas` que ya existan en ese MongoDB antes de importar el respaldo. No lo uses sobre datos que quieras conservar sin hacer antes otro respaldo (ver [Respaldo de MongoDB](#-respaldo-de-mongodb)).
>
> Si no quieres cargar datos y prefieres empezar con la base vacía, omite este paso.

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

Desde ahí puedes:

* **Enviar una transacción manual:** completa el formulario y pulsa **"Enviar transacción manual"**. Para probar una alerta, envía un retiro, depósito o transferencia **mayor a $800.000** (por ejemplo, `1000000`).
* **Generar tráfico automático:** escribe un nombre en **"Usuario que genera los eventos"** y pulsa **"Iniciar envío automático"**. Para parar, pulsa **"Detener envío automático"**.

> Kafka debe estar encendido antes de iniciar `app.py`, porque el productor de Kafka se crea al arrancar la aplicación.

## 7. (Opcional) Ejecutar el productor por consola (Terminal 4)

El dashboard ya trae su propio simulador automático (paso 6). Si prefieres generar tráfico continuo desde la consola, sin abrir el navegador:

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

* Las tarjetas **Transacciones enviadas / confirmadas / Errores de envío** suben con cada envío (manual o automático) y **Total de alertas** refleja las alertas guardadas.
* **Últimos envíos** y **Historial de transacciones** muestran lo guardado en MongoDB, de la más reciente a la más antigua.
* **Alertas de seguridad** lista las transacciones que superaron `$800.000` COP.
* Los datos pueden tardar unos segundos en aparecer (Spark procesa por micro-batches y el dashboard consulta cada 5 s).

**Verificación directa en MongoDB** (opcional):

```powershell
docker exec -it atm-mongodb mongosh monitoreo_atm --eval "db.transacciones.countDocuments(); db.alertas.countDocuments()"
```

> Los datos del simulador son aleatorios, así que los resultados cambian en cada ejecución.

---

# 🔄 Cómo funciona

1. **Generación:** el navegador crea la transacción (a mano desde el formulario o con el simulador automático del dashboard) y la envía a Flask, que la valida y arma el evento; opcionalmente, `productor.py` crea transacciones aleatorias desde la consola.
2. **Envío:** ambos serializan el evento a JSON y lo publican en el tópico `transacciones-atm` mediante `localhost:9092`. El dashboard espera la confirmación de Kafka antes de responder `201`.
3. **Recepción:** Kafka almacena los eventos y los deja disponibles para los consumidores.
4. **Procesamiento:** Spark se conecta por la red interna de Docker (`kafka:29092`), lee los eventos y convierte el JSON a columnas usando un esquema definido.
5. **Almacenamiento:** en cada micro-batch, `procesar_lote` guarda cada transacción en MongoDB (`atm-mongodb:27017`) y, si supera el umbral, registra una alerta.
6. **Visualización:** el dashboard consulta MongoDB (`localhost:27017`) a través de la API y actualiza el historial, las alertas y los últimos envíos cada 5 segundos.

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

El simulador automático del dashboard usa este mismo endpoint (una llamada por transacción).

Ejemplo de envío desde PowerShell:

```powershell
Invoke-RestMethod -Uri http://localhost:5000/api/transacciones `
  -Method Post -ContentType "application/json" `
  -Body '{"atm_id":"ATM-001","transaction_type":"RETIRO","amount":1000000,"usuario":"Prueba"}'
```

---

# 💾 Respaldo de MongoDB

El respaldo se guarda en `backups/monitoreo_atm.archive` (formato `mongodump`, comprimido con gzip) y se versiona en GitHub para poder llevar los mismos datos a otro computador. `mongodump` y `mongorestore` ya vienen dentro de la imagen `mongo:7`, no hace falta instalar nada más.

### Crear o actualizar el respaldo (computador de origen)

Con `atm-mongodb` en ejecución:

```powershell
cd "$HOME\Desktop\ejercicio-atm"
New-Item -ItemType Directory -Force .\backups

docker exec atm-mongodb mongodump --db monitoreo_atm --archive=/tmp/monitoreo_atm.archive --gzip
docker cp atm-mongodb:/tmp/monitoreo_atm.archive .\backups\monitoreo_atm.archive
```

### Subirlo a GitHub

`.gitignore` excluye `*.archive` para no subir respaldos por accidente. El archivo del proyecto ya está registrado en Git, así que los cambios se detectan normalmente; si fuera la primera vez, o Git lo ignora, fuerza su inclusión:

```powershell
git add -f backups/monitoreo_atm.archive
git commit -m "Actualizar respaldo de MongoDB"
git push origin main
```

> Si tu rama no se llama `main`, consulta la actual con `git branch --show-current`.
> ⚠️ En un repositorio público cualquiera puede descargar el respaldo. Antes de subirlo, revisa que no contenga datos sensibles (los campos `usuario` guardan los nombres escritos en el dashboard).

### Restaurarlo en otro computador

Es el [paso 2.1](#21-restaurar-los-datos-de-mongodb-respaldo-incluido):

```powershell
git pull        # si ya tenías el proyecto clonado
docker compose up -d
docker cp .\backups\monitoreo_atm.archive atm-mongodb:/tmp/monitoreo_atm.archive
docker exec atm-mongodb mongorestore --archive=/tmp/monitoreo_atm.archive --gzip --drop
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

> Si hiciste esto, puedes recuperar los datos con el respaldo del [paso 2.1](#21-restaurar-los-datos-de-mongodb-respaldo-incluido).

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
| Envío exitoso pero el historial y los últimos envíos no muestran datos | Spark no está corriendo, o se inició después del envío (offset `latest`) | Deja Spark corriendo **antes** de enviar transacciones y espera unos segundos. |
| No aparecen alertas | El monto no supera `$800.000` o es una consulta de saldo | Envía un retiro, depósito o transferencia mayor a `800000`. |
| El simulador automático no inicia y pide un nombre | El campo "Usuario que genera los eventos" está vacío | Escribe un nombre y vuelve a pulsar "Iniciar envío automático". |
| El contador "Total de alertas" no pasa de 50 | La API devuelve solo las 50 alertas más recientes | Es normal; para el total real consulta MongoDB: `docker exec -it atm-mongodb mongosh monitoreo_atm --eval "db.alertas.countDocuments()"`. |
| `mongorestore` falla con `no such file` o `Failed: ... archive` | El archivo no se copió al contenedor o el nombre no coincide | Ejecuta primero `docker cp` desde la carpeta del proyecto y verifica que exista `backups\monitoreo_atm.archive`. |
| Tras restaurar, el dashboard sigue vacío | Se restauró en otro MongoDB o el contenedor se recreó con `down -v` después | Verifica con el comando de conteo del paso 2.1 y repite la restauración. |
| `git pull` no trae el respaldo o aparece como ignorado | `.gitignore` excluye `*.archive` | Usa `git add -f backups/monitoreo_atm.archive` al subirlo. |
| Falla la descarga de `--packages` | Sin internet o problema de permisos en `.ivy2` | Revisa la conexión, confirma que la carpeta `.ivy2` existe y vuelve a ejecutar. |
| `${PWD}` no se reconoce | Estás usando CMD en lugar de PowerShell | Usa PowerShell, o reemplaza `${PWD}` por la ruta completa del proyecto. |
| Puerto `9092`, `27017` o `5000` ocupado | Otro servicio usa ese puerto | Cierra el otro servicio o cambia el puerto en `docker-compose.yml` / `app.py` (y en el código que lo usa). |

---

# 👨‍💻 Autor

**Ejercicio académico: Monitoreo de Redes de Cajeros Automáticos (ATMs)**

Tecnologías: **Python + Flask + Apache Kafka + Apache Spark + MongoDB + Docker**