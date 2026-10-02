# 🏧 Monitoreo de Redes de Cajeros Automáticos (ATMs)

Ejercicio práctico de **procesamiento de datos en tiempo real** con **Python, Apache Kafka y Apache Spark Structured Streaming**, todo ejecutado con **Docker**.

Se simulan transacciones de una red de cajeros automáticos, se envían a Kafka y Spark las procesa para mostrar, por cada cajero, cuántas transacciones tuvo (por tipo) y cuánto dinero movió.

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
9. [Detener y limpiar](#-detener-y-limpiar)
10. [Solución de problemas](#-solución-de-problemas)

---

## 🎯 Objetivo y arquitectura

Construir un flujo básico de procesamiento de eventos en tiempo real:

```text
🏧 Productor Python (productor.py)
        │   localhost:9092
        ▼
📨 Apache Kafka  (tópico: transacciones-atm)
        │   kafka:29092
        ▼
⚡ Spark Structured Streaming (spark_consumer.py)
        │
        ▼
📊 Resultados agregados por cajero (consola)
```

Tipos de operación simulados:

* `RETIRO`
* `DEPOSITO`
* `CONSULTA_SALDO` (no mueve dinero, monto = 0)
* `TRANSFERENCIA`

Ejemplo de evento:

```json
{
  "atm_id": "ATM-003",
  "transaction_id": "TX-0084",
  "transaction_type": "TRANSFERENCIA",
  "amount": 500000,
  "currency": "COP",
  "timestamp": "2026-10-01 21:45:30"
}
```

---

## 🛠️ Tecnologías

| Tecnología               | Uso                                         |
| ------------------------ | ------------------------------------------- |
| Python 3.12              | Generación de transacciones                 |
| kafka-python             | Comunicación entre Python y Kafka           |
| Apache Kafka             | Mensajería y transmisión de eventos         |
| Apache Spark 3.5.7       | Procesamiento de datos                      |
| Spark Structured Streaming | Procesamiento continuo de transacciones   |
| Docker / Docker Compose  | Ejecución de Kafka y Spark                  |
| PowerShell               | Ejecución de comandos en Windows            |

---

## ✅ Requisitos previos

Instalar antes de empezar:

| Programa | Para qué | Descarga |
| -------- | -------- | -------- |
| **Docker Desktop** | Correr Kafka y Spark en contenedores | https://www.docker.com/products/docker-desktop/ |
| **Python 3.12** | Ejecutar el productor | https://www.python.org/downloads/ |
| **Git** (opcional) | Clonar el repositorio | https://git-scm.com/downloads |

Notas importantes:

* Al instalar Python en Windows, marca la casilla **"Add Python to PATH"**.
* Después de instalar Docker Desktop, **ábrelo y espera a que diga "Engine running"** antes de continuar.
* Se necesita **conexión a internet** la primera vez (se descargan imágenes de Docker y paquetes de Spark).
* Recomendado: al menos **4 GB de RAM** libres para Docker.

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

> ⚠️ **Sobre el nombre de la carpeta:** Docker Compose nombra la red como `<nombre-de-la-carpeta>_atm-network`. Con la carpeta `ejercicio-atm`, la red será `ejercicio-atm_atm-network`, que es la que usa el comando de Spark en el paso 6. Si cambias el nombre de la carpeta, ajusta ese comando (ver [solución de problemas](#-solución-de-problemas)).

---

## 📁 Estructura del proyecto

```text
ejercicio-atm/
│
├── docker-compose.yml     # Contenedor de Kafka y su red
├── productor.py           # Simulador de transacciones ATM
├── spark_consumer.py      # Procesamiento con Spark Streaming
└── .ivy2/                 # Caché de paquetes de Spark (se crea en el paso 6)
```

### `docker-compose.yml`

Levanta un contenedor de Apache Kafka (`atm-kafka`) en modo KRaft (sin Zookeeper) con **dos listeners**:

```text
EXTERNAL → localhost:9092   (para Python en Windows)
INTERNAL → kafka:29092      (para Spark dentro de Docker)
```

Los tópicos nuevos se crean por defecto con **3 particiones** y **1 réplica**.

### `productor.py`

Simula 5 cajeros (`ATM-001` … `ATM-005`), genera una transacción aleatoria cada 1 a 3 segundos y la envía al tópico `transacciones-atm`.

### `spark_consumer.py`

Lee el tópico con Spark Structured Streaming, agrupa por `atm_id` y calcula por cajero:

* `total_transacciones`
* `retiros`
* `depositos`
* `transferencias`
* `consultas_saldo`
* `dinero_movido` (suma de `amount`)

---

# 🚀 Instalación y ejecución paso a paso

> Necesitarás **tres ventanas de PowerShell** abiertas en la carpeta del proyecto:
> una para Kafka/comandos, una para el productor y una para Spark.

## 1. Instalar la dependencia de Python

Se recomienda usar un entorno virtual:

```powershell
cd "$HOME\Desktop\ejercicio-atm"
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install kafka-python
```

> Si PowerShell bloquea la activación del entorno, ejecuta una vez:
> `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`
> y vuelve a activar el entorno.

Si prefieres no usar entorno virtual, basta con `pip install kafka-python`.

## 2. Levantar Kafka

Con Docker Desktop abierto:

```powershell
docker compose up -d
```

La primera vez descargará la imagen `apache/kafka`. Verifica que esté corriendo:

```powershell
docker ps
```

Debe aparecer el contenedor `atm-kafka` con estado `Up`.

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

Si **no aparece** `transacciones-atm`, créalo manualmente (también se crea automáticamente cuando el productor envía el primer mensaje):

```powershell
docker exec -it atm-kafka /opt/kafka/bin/kafka-topics.sh --create --topic transacciones-atm --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

## 4. Crear la carpeta de caché de Spark

Solo la primera vez:

```powershell
mkdir .ivy2
```

## 5. Ejecutar Spark Streaming (Terminal 2)

Abre una **nueva PowerShell**, entra a la carpeta del proyecto y ejecuta:

```powershell
cd "$HOME\Desktop\ejercicio-atm"

docker run --rm -it `
  --network ejercicio-atm_atm-network `
  -v "${PWD}:/app" `
  -v "${PWD}\.ivy2:/tmp/.ivy2" `
  -e HOME=/tmp `
  apache/spark:3.5.7 `
  /opt/spark/bin/spark-submit `
  --conf spark.jars.ivy=/tmp/.ivy2 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 `
  /app/spark_consumer.py
```

* La **primera vez** tarda unos minutos porque descarga la imagen de Spark y el conector de Kafka (queda guardado en `.ivy2`).
* Spark queda escuchando el tópico `transacciones-atm`. Al principio no mostrará datos hasta que el productor empiece a enviar.
* El consumidor usa `startingOffsets = latest`, por lo que **solo cuenta las transacciones que lleguen después de que Spark arranque**.

## 6. Ejecutar el productor (Terminal 3)

Abre otra **PowerShell**:

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
🏧 ATM-002 | DEPOSITO | $300,000 COP | TX-0003
```

Para detenerlo: `Ctrl + C`.

---

# 📊 Resultado esperado

En la terminal de Spark irán apareciendo batches con los resultados actualizados:

```text
-------------------------------------------
Batch: 12
-------------------------------------------
+-------+-------------------+-------+---------+--------------+---------------+-------------+
|atm_id |total_transacciones|retiros|depositos|transferencias|consultas_saldo|dinero_movido|
+-------+-------------------+-------+---------+--------------+---------------+-------------+
|ATM-002|5                  |2      |1        |1             |1              |850000       |
|ATM-005|6                  |1      |2        |2             |1              |1200000      |
|ATM-004|4                  |1      |1        |1             |1              |1500000      |
|ATM-001|3                  |1      |1        |0             |1              |400000       |
|ATM-003|7                  |2      |2        |2             |1              |2150000      |
+-------+-------------------+-------+---------+--------------+---------------+-------------+
```

> Los números cambian en cada ejecución porque los datos son aleatorios. Las cifras de `dinero_movido` suman los montos de retiros, depósitos y transferencias (las consultas de saldo suman 0).

---

# 🔄 Cómo funciona

1. **Generación:** `productor.py` crea una transacción aleatoria (cajero, tipo, monto, fecha).
2. **Envío:** la serializa a JSON y la publica en el tópico `transacciones-atm` mediante `localhost:9092`.
3. **Recepción:** Kafka almacena los eventos y los deja disponibles para los consumidores.
4. **Procesamiento:** Spark se conecta por la red interna de Docker (`kafka:29092`), lee los eventos y convierte el JSON a columnas usando un esquema definido.
5. **Agregación:** agrupa por `atm_id` y calcula conteos por tipo y el dinero total movido.
6. **Visualización:** con `outputMode("complete")` imprime en consola la tabla completa actualizada en cada batch.

### Conceptos aplicados

* **Apache Kafka:** broker de mensajería con 3 particiones y 1 réplica; dos listeners (`EXTERNAL` e `INTERNAL`) para que Windows y los contenedores puedan conectarse.
* **Spark Structured Streaming:** trata el flujo como una tabla que crece continuamente y procesa los datos en micro-batches (por eso aparecen `Batch: 1`, `Batch: 2`, ...).
* **Agregaciones con estado:** `groupBy("atm_id")` con `count`, `sum` y `when` mantiene los totales acumulados entre batches.

---

# 🧹 Detener y limpiar

1. Detener el productor y Spark con `Ctrl + C` en cada terminal.
2. Apagar Kafka y la red:

```powershell
docker compose down
```

3. (Opcional) Borrar la caché de paquetes de Spark:

```powershell
Remove-Item -Recurse -Force .ivy2
```

Para volver a ejecutar todo desde cero, repite desde el [paso 2](#2-levantar-kafka).

---

# 🩺 Solución de problemas

| Problema | Causa probable | Solución |
| -------- | -------------- | -------- |
| `error during connect` / Docker no responde | Docker Desktop no está abierto | Abre Docker Desktop y espera a que diga "Engine running". |
| `NoBrokersAvailable` en el productor | Kafka aún no arrancó o está apagado | Revisa `docker ps`, espera unos segundos y vuelve a intentar. Mira los logs con `docker logs atm-kafka`. |
| `ModuleNotFoundError: No module named 'kafka'` | Falta instalar la librería o el entorno no está activo | Activa el entorno (`.\venv\Scripts\Activate.ps1`) y ejecuta `pip install kafka-python`. |
| `No module named 'kafka.vendor.six.moves'` | Versión antigua de `kafka-python` incompatible con Python 3.12 | `pip install --upgrade kafka-python` (o `pip install kafka-python-ng`). |
| `network ejercicio-atm_atm-network not found` | La carpeta tiene otro nombre o Kafka no está levantado | Ejecuta `docker network ls`, copia el nombre real de la red terminada en `_atm-network` y úsalo en el comando de Spark. |
| Spark no muestra datos | El productor no está corriendo, o se inició antes que Spark (offset `latest`) | Deja Spark corriendo y asegúrate de que el productor siga enviando; las transacciones nuevas aparecerán en el siguiente batch. |
| Spark no puede resolver `kafka:29092` | Spark no está en la misma red de Docker | Verifica que el comando incluya `--network ejercicio-atm_atm-network`. |
| Falla la descarga de `--packages` | Sin internet o problema de permisos en `.ivy2` | Revisa la conexión, confirma que la carpeta `.ivy2` existe y vuelve a ejecutar. |
| `${PWD}` no se reconoce | Estás usando CMD en lugar de PowerShell | Usa PowerShell, o reemplaza `${PWD}` por la ruta completa del proyecto. |
| Puerto `9092` ocupado | Otro servicio usa ese puerto | Cierra el otro servicio o cambia el puerto en `docker-compose.yml` y en `productor.py`. |

---

# 👨‍💻 Autor

**Ejercicio académico: Monitoreo de Redes de Cajeros Automáticos (ATMs)**

Tecnologías: **Python + Apache Kafka + Apache Spark + Docker**
