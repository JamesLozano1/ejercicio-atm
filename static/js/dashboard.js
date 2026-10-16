
const formulario = document.getElementById("formTransaccion");
const tipoTransaccion = document.getElementById("transaction_type");
const campoMonto = document.getElementById("amount");
const ayudaMonto = document.getElementById("ayudaMonto");
const botonEnviar = document.getElementById("btnEnviar");
const mensaje = document.getElementById("mensaje");
const tabla = document.getElementById("tablaTransacciones");

const tablaHistorial = document.getElementById("tablaHistorial");
const tablaAlertas = document.getElementById("tablaAlertas");
const estadoHistorial = document.getElementById("estadoHistorial");
const totalAlertasElemento = document.getElementById("totalAlertas");

let totalEnviadas = 0;
let totalConfirmadas = 0;
let totalErrores = 0;
let actualizandoDatos = false;

const formatoCOP = new Intl.NumberFormat("es-CO", {
    style: "currency",
    currency: "COP",
    maximumFractionDigits: 0
});

document.getElementById("fechaActual").textContent =
    new Date().toLocaleDateString("es-CO", {
        year: "numeric",
        month: "short",
        day: "numeric"
    });

function actualizarContadores() {
    document.getElementById("totalEnviadas").textContent = totalEnviadas;
    document.getElementById("totalConfirmadas").textContent = totalConfirmadas;
    document.getElementById("totalErrores").textContent = totalErrores;
}

function actualizarMonto() {
    const esConsulta = tipoTransaccion.value === "CONSULTA_SALDO";

    campoMonto.disabled = esConsulta;
    campoMonto.required = !esConsulta;
    campoMonto.value = esConsulta ? 0 : 50000;

    ayudaMonto.textContent = esConsulta
        ? "La consulta de saldo no mueve dinero. Su monto será $0 COP."
        : "Ingresa el valor en pesos colombianos.";
}

tipoTransaccion.addEventListener("change", actualizarMonto);
actualizarMonto();

formulario.addEventListener("submit", async (evento) => {
    evento.preventDefault();

    totalEnviadas++;
    actualizarContadores();

    botonEnviar.disabled = true;
    botonEnviar.textContent = "Enviando...";
    mensaje.textContent = "Enviando transacción al servidor...";
    mensaje.className = "message";

    const datos = {
        usuario: document.getElementById("usuario").value.trim() || "Usuario web",
        atm_id: document.getElementById("atm_id").value,
        transaction_type: tipoTransaccion.value,
        amount: Number(campoMonto.value)
    };

    try {
        const respuesta = await fetch("/api/transacciones", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify(datos)
        });

        const resultado = await respuesta.json();

        if (!respuesta.ok) {
            throw new Error(
                resultado.error || "No fue posible enviar la transacción."
            );
        }

        totalConfirmadas++;
        mensaje.textContent = resultado.mensaje || "Transacción enviada a Kafka.";
        mensaje.className = "message success";

        if (resultado.transaccion) {
            agregarFila(resultado.transaccion);
        }

        formulario.reset();
        actualizarMonto();

        // Actualizar el historial y las alertas después del envío.
        await actualizarDashboard();

    } catch (error) {
        totalErrores++;
        mensaje.textContent = error.message || "Ocurrió un error al enviar.";
        mensaje.className = "message error";
    } finally {
        actualizarContadores();
        botonEnviar.disabled = false;
        botonEnviar.textContent = "Enviar a Kafka";
    }
});

// Tabla de transacciones enviadas durante la sesión actual.
function agregarFila(transaccion) {
    const filaVacia = document.getElementById("filaVacia");
    if (filaVacia) filaVacia.remove();

    const fila = document.createElement("tr");

    const valores = [
        transaccion.atm_id || "N/D",
        transaccion.transaction_type || "N/D",
        formatoCOP.format(Number(transaccion.amount) || 0),
        "Confirmada"
    ];

    valores.forEach((valor, indice) => {
        const celda = document.createElement("td");
        celda.textContent = valor;

        if (indice === 3) {
            celda.className = "state-ok";
        }

        fila.appendChild(celda);
    });

    tabla.prepend(fila);

    while (tabla.rows.length > 10) {
        tabla.deleteRow(tabla.rows.length - 1);
    }
}

// Convierte las fechas recibidas del servidor a una presentación legible.
function formatearFecha(fecha) {
    if (!fecha) return "N/D";

    const fechaConvertida = new Date(fecha);

    if (Number.isNaN(fechaConvertida.getTime())) {
        return String(fecha);
    }

    return fechaConvertida.toLocaleString("es-CO", {
        dateStyle: "short",
        timeStyle: "medium"
    });
}

// Inserta texto en una celda de forma segura.
function agregarCelda(fila, valor, clase = "") {
    const celda = document.createElement("td");
    celda.textContent = valor ?? "N/D";

    if (clase) {
        celda.className = clase;
    }

    fila.appendChild(celda);
    return celda;
}

function mostrarFilaVacia(cuerpo, columnas, texto) {
    cuerpo.replaceChildren();

    const fila = document.createElement("tr");
    const celda = document.createElement("td");

    celda.colSpan = columnas;
    celda.textContent = texto;
    celda.style.textAlign = "center";

    fila.appendChild(celda);
    cuerpo.appendChild(fila);
}

// Consulta las transacciones persistidas en MongoDB mediante Flask.
async function cargarHistorial() {
    if (!tablaHistorial) return;

    try {
        const respuesta = await fetch("/api/transacciones", {
            cache: "no-store"
        });

        if (!respuesta.ok) {
            throw new Error("El servidor no pudo consultar el historial.");
        }

        const resultado = await respuesta.json();
        const transacciones = resultado.transacciones || [];

        tablaHistorial.replaceChildren();

        if (transacciones.length === 0) {
            mostrarFilaVacia(
                tablaHistorial,
                6,
                "Todavía no hay transacciones guardadas en MongoDB."
            );
        } else {
            transacciones.forEach((transaccion) => {
                const fila = document.createElement("tr");

                agregarCelda(
                    fila,
                    formatearFecha(
                        transaccion.fecha_procesamiento ||
                        transaccion.timestamp ||
                        transaccion.fecha
                    )
                );

                agregarCelda(
                    fila,
                    transaccion.transaction_id || transaccion.transaccion_id || "N/D"
                );

                agregarCelda(fila, transaccion.atm_id || "N/D");
                agregarCelda(fila, transaccion.transaction_type || "N/D");

                agregarCelda(
                    fila,
                    formatoCOP.format(Number(transaccion.amount) || 0)
                );

                agregarCelda(
                    fila,
                    transaccion.usuario || transaccion.user || "N/D"
                );

                tablaHistorial.appendChild(fila);
            });
        }

        if (estadoHistorial) {
            estadoHistorial.textContent =
                `Actualizado ${new Date().toLocaleTimeString("es-CO")}`;
            estadoHistorial.className = "state-ok";
        }
    } catch (error) {
        if (estadoHistorial) {
            estadoHistorial.textContent = "Error al actualizar el historial";
            estadoHistorial.className = "state-error";
        }

        console.error("Error consultando el historial:", error);
    }
}

// Consulta las alertas detectadas por Spark y guardadas en MongoDB.
async function cargarAlertas() {
    if (!tablaAlertas) return;

    try {
        const respuesta = await fetch("/api/alertas", {
            cache: "no-store"
        });

        if (!respuesta.ok) {
            throw new Error("El servidor no pudo consultar las alertas.");
        }

        const resultado = await respuesta.json();
        const alertas = resultado.alertas || [];

        if (totalAlertasElemento) {
            totalAlertasElemento.textContent = resultado.total ?? alertas.length;
        }

        tablaAlertas.replaceChildren();

        if (alertas.length === 0) {
            mostrarFilaVacia(
                tablaAlertas,
                6,
                "No hay alertas registradas."
            );
            return;
        }

        alertas.forEach((alerta) => {
            const fila = document.createElement("tr");

            agregarCelda(
                fila,
                formatearFecha(
                    alerta.fecha_deteccion ||
                    alerta.timestamp ||
                    alerta.fecha
                )
            );

            agregarCelda(fila, alerta.atm_id || "N/D");
            agregarCelda(
                fila,
                alerta.transaction_type || alerta.tipo_transaccion || "N/D"
            );

            agregarCelda(
                fila,
                formatoCOP.format(
                    Number(alerta.amount ?? alerta.monto) || 0
                )
            );

            const severidad = String(
                alerta.severidad || alerta.severity || "ALTA"
            ).toUpperCase();

            const celdaSeveridad = agregarCelda(fila, severidad);
            celdaSeveridad.className =
                severidad === "CRITICA" || severidad === "CRÍTICA"
                    ? "state-error"
                    : "state-warning";

            agregarCelda(
                fila,
                alerta.descripcion ||
                alerta.mensaje ||
                alerta.motivo ||
                "Transacción que supera el umbral permitido."
            );

            tablaAlertas.appendChild(fila);
        });
    } catch (error) {
        mostrarFilaVacia(
            tablaAlertas,
            6,
            "No fue posible cargar las alertas. Revisa Flask y MongoDB."
        );

        console.error("Error consultando las alertas:", error);
    }
}


async function cargarUltimosEnvios() {
    if (!tabla) return;

    try {
        const respuesta = await fetch("/api/transacciones", {
            cache: "no-store"
        });

        if (!respuesta.ok) {
            throw new Error("No se pudieron recuperar los últimos envíos.");
        }

        const resultado = await respuesta.json();
        const transacciones = (resultado.transacciones || []).slice(0, 10);

        tabla.replaceChildren();

        if (transacciones.length === 0) {
            mostrarFilaVacia(
                tabla,
                4,
                "Todavía no hay transacciones registradas."
            );
            return;
        }

        transacciones.forEach((transaccion) => {
            const fila = document.createElement("tr");

            agregarCelda(fila, transaccion.atm_id || "N/D");
            agregarCelda(fila, transaccion.transaction_type || "N/D");

            agregarCelda(
                fila,
                formatoCOP.format(Number(transaccion.amount) || 0)
            );

            const celdaEstado = agregarCelda(fila, "Registrada");
            celdaEstado.className = "state-ok";

            tabla.appendChild(fila);
        });
    } catch (error) {
        console.error("Error recuperando los últimos envíos:", error);
    }
}

// Evita que se solapen varias actualizaciones del dashboard.

async function actualizarDashboard() {
    if (actualizandoDatos) return;

    actualizandoDatos = true;

    try {
        await Promise.all([
            cargarHistorial(),
            cargarAlertas()
        ]);

        // Restaurar los últimos 10 envíos desde MongoDB.
        await cargarUltimosEnvios();
    } finally {
        actualizandoDatos = false;
    }
}

// Primera consulta al abrir el dashboard y actualización cada cinco segundos.
actualizarDashboard();
setInterval(actualizarDashboard, 5000);
