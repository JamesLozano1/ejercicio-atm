// ============================================================
// MONITOREO DE REDES DE CAJEROS AUTOMÁTICOS (ATM)
// Flask + Kafka + Spark Structured Streaming + MongoDB
// ============================================================


// ============================================================
// 1. ELEMENTOS DEL HTML
// ============================================================

const formTransaccion = document.getElementById("formTransaccion");

const inputUsuario = document.getElementById("usuario");
const inputATM = document.getElementById("atm_id");
const inputTipo = document.getElementById("transaction_type");
const inputMonto = document.getElementById("amount");
const ayudaMonto = document.getElementById("ayudaMonto");

const inputUsuarioAutomatico =
    document.getElementById("usuarioAutomatico");

const btnEnviar = document.getElementById("btnEnviar");
const btnIniciarAutomatico =
    document.getElementById("btnIniciarAutomatico");
const btnDetenerAutomatico =
    document.getElementById("btnDetenerAutomatico");

const estadoAutomatico =
    document.getElementById("estadoAutomatico");
const mensaje = document.getElementById("mensaje");

const totalEnviadasElemento =
    document.getElementById("totalEnviadas");
const totalConfirmadasElemento =
    document.getElementById("totalConfirmadas");
const totalErroresElemento =
    document.getElementById("totalErrores");
const totalAlertasElemento =
    document.getElementById("totalAlertas");

const tablaTransacciones =
    document.getElementById("tablaTransacciones");
const tablaHistorial =
    document.getElementById("tablaHistorial");
const tablaAlertas =
    document.getElementById("tablaAlertas");

const estadoHistorial =
    document.getElementById("estadoHistorial");
const fechaActual =
    document.getElementById("fechaActual");


// ============================================================
// 2. VARIABLES DEL SISTEMA
// ============================================================

let totalEnviadas = 0;
let totalConfirmadas = 0;
let totalErrores = 0;

let envioAutomaticoActivo = false;
let envioManualEnCurso = false;
let actualizandoDatos = false;


// ============================================================
// 3. FORMATO DE MONEDA Y FECHAS
// ============================================================

const formatoCOP = new Intl.NumberFormat("es-CO", {
    style: "currency",
    currency: "COP",
    maximumFractionDigits: 0
});

function formatearFecha(fecha) {
    if (!fecha) {
        return "Sin fecha";
    }

    const valor = new Date(fecha);

    if (Number.isNaN(valor.getTime())) {
        return String(fecha);
    }

    return valor.toLocaleString("es-CO");
}

function actualizarFechaActual() {
    if (!fechaActual) return;

    fechaActual.textContent = new Date().toLocaleDateString(
        "es-CO",
        {
            weekday: "long",
            year: "numeric",
            month: "long",
            day: "numeric"
        }
    );
}


// ============================================================
// 4. CONTADORES Y MENSAJES
// ============================================================

function actualizarContadores() {
    if (totalEnviadasElemento) {
        totalEnviadasElemento.textContent = totalEnviadas;
    }

    if (totalConfirmadasElemento) {
        totalConfirmadasElemento.textContent = totalConfirmadas;
    }

    if (totalErroresElemento) {
        totalErroresElemento.textContent = totalErrores;
    }
}

function mostrarMensaje(texto, tipo = "info") {
    if (!mensaje) return;

    mensaje.textContent = texto;
    mensaje.dataset.tipo = tipo;
}


// ============================================================
// 5. UTILIDADES PARA MOSTRAR DATOS DE FORMA SEGURA
// ============================================================

function crearCelda(valor) {
    const celda = document.createElement("td");

    celda.textContent =
        valor === undefined || valor === null || valor === ""
            ? "—"
            : String(valor);

    return celda;
}

function obtenerCampo(objeto, nombres, predeterminado = "") {
    for (const nombre of nombres) {
        if (
            objeto &&
            objeto[nombre] !== undefined &&
            objeto[nombre] !== null
        ) {
            return objeto[nombre];
        }
    }

    return predeterminado;
}

function normalizarTransaccion(transaccion) {
    const monto = Number(
        obtenerCampo(transaccion, ["amount", "monto"], 0)
    );

    return {
        fecha: obtenerCampo(
            transaccion,
            [
                "fecha_procesamiento",
                "fecha",
                "timestamp",
                "created_at"
            ],
            ""
        ),

        id: obtenerCampo(
            transaccion,
            [
                "transaction_id",
                "transaccion_id",
                "id_transaccion",
                "id"
            ],
            "—"
        ),

        atm: obtenerCampo(
            transaccion,
            ["atm_id", "cajero_id", "atm"],
            "—"
        ),

        tipo: obtenerCampo(
            transaccion,
            [
                "transaction_type",
                "tipo_transaccion",
                "tipo"
            ],
            "—"
        ),

        monto: Number.isFinite(monto) ? monto : 0,

        usuario: obtenerCampo(
            transaccion,
            ["usuario", "user", "username"],
            "—"
        )
    };
}

function mostrarFilaVacia(tabla, columnas, texto) {
    if (!tabla) return;

    const fila = document.createElement("tr");
    const celda = document.createElement("td");

    celda.colSpan = columnas;
    celda.textContent = texto;

    fila.appendChild(celda);
    tabla.replaceChildren(fila);
}


// ============================================================
// 6. TABLA DE ÚLTIMOS ENVÍOS
// ============================================================

function agregarFila(transaccion) {
    if (!tablaTransacciones) return;

    const datos = normalizarTransaccion(transaccion);
    const fila = document.createElement("tr");

    fila.appendChild(crearCelda(formatearFecha(datos.fecha)));
    fila.appendChild(crearCelda(datos.atm));
    fila.appendChild(crearCelda(datos.tipo));
    fila.appendChild(crearCelda(formatoCOP.format(datos.monto)));
    fila.appendChild(crearCelda(datos.usuario));

    tablaTransacciones.prepend(fila);

    while (tablaTransacciones.rows.length > 10) {
        tablaTransacciones.deleteRow(
            tablaTransacciones.rows.length - 1
        );
    }
}


// ============================================================
// 7. ENVIAR UNA TRANSACCIÓN A FLASK
// ============================================================

async function enviarTransaccion(datos) {
    const respuesta = await fetch("/api/transacciones", {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify(datos)
    });

    let resultado;

    try {
        resultado = await respuesta.json();
    } catch {
        throw new Error(
            "El servidor no devolvió una respuesta válida."
        );
    }

    if (!respuesta.ok) {
        throw new Error(
            resultado.error ||
            resultado.mensaje ||
            `Error HTTP ${respuesta.status}`
        );
    }

    totalConfirmadas++;
    actualizarContadores();

    const transaccionDevuelta =
        resultado.transaccion ||
        resultado.data ||
        datos;

    agregarFila(transaccionDevuelta);

    return resultado;
}


// ============================================================
// 8. ENVÍO MANUAL
// ============================================================

if (formTransaccion) {
    formTransaccion.addEventListener("submit", async function (evento) {
        evento.preventDefault();

        if (envioManualEnCurso || envioAutomaticoActivo) {
            return;
        }

        const usuario = inputUsuario.value.trim();
        const tipo = inputTipo.value;

        const datos = {
            usuario: usuario,
            atm_id: inputATM.value,
            transaction_type: tipo,
            amount: tipo === "CONSULTA_SALDO"
                ? 0
                : Number(inputMonto.value)
        };

        if (!usuario) {
            mostrarMensaje(
                "Escribe el nombre del usuario.",
                "error"
            );
            inputUsuario.focus();
            return;
        }

        if (
            !Number.isFinite(datos.amount) ||
            datos.amount < 0
        ) {
            mostrarMensaje(
                "Ingresa un monto válido.",
                "error"
            );
            inputMonto.focus();
            return;
        }

        envioManualEnCurso = true;
        totalEnviadas++;
        actualizarContadores();

        if (btnEnviar) {
            btnEnviar.disabled = true;
            btnEnviar.textContent = "Enviando...";
        }

        try {
            await enviarTransaccion(datos);

            mostrarMensaje(
                "Transacción enviada correctamente a Kafka.",
                "success"
            );

            await actualizarDashboard();

        } catch (error) {
            totalErrores++;
            actualizarContadores();

            mostrarMensaje(
                `Error al enviar la transacción: ${error.message}`,
                "error"
            );

            console.error("Error en el envío manual:", error);

        } finally {
            envioManualEnCurso = false;

            if (btnEnviar) {
                btnEnviar.disabled = envioAutomaticoActivo;
                btnEnviar.textContent =
                    "Enviar transacción manual";
            }
        }
    });
}


// ============================================================
// 9. CONTROL DEL MONTO
// ============================================================

function actualizarCampoMonto() {
    if (!inputTipo || !inputMonto) return;

    const esConsulta =
        inputTipo.value === "CONSULTA_SALDO";

    if (esConsulta) {
        inputMonto.value = 0;
        inputMonto.disabled = true;

        if (ayudaMonto) {
            ayudaMonto.textContent =
                "La consulta de saldo se registra con monto de $0 COP.";
        }
    } else {
        inputMonto.disabled = false;

        if (ayudaMonto) {
            ayudaMonto.textContent =
                "Puedes usar montos superiores a $800.000 COP para probar las alertas.";
        }
    }
}

if (inputTipo) {
    inputTipo.addEventListener(
        "change",
        actualizarCampoMonto
    );
}

actualizarCampoMonto();


// ============================================================
// 10. GENERAR TRANSACCIONES AUTOMÁTICAS
// ============================================================

function generarTransaccionAutomatica() {
    const cajeros = [
        "ATM-001",
        "ATM-002",
        "ATM-003",
        "ATM-004",
        "ATM-005"
    ];

    const tipos = [
        "RETIRO",
        "DEPOSITO",
        "CONSULTA_SALDO",
        "TRANSFERENCIA"
    ];

    const tipo =
        tipos[Math.floor(Math.random() * tipos.length)];

    let monto = 0;

    if (tipo !== "CONSULTA_SALDO") {
        // Aproximadamente el 30 % de las operaciones monetarias
        // supera los $800.000 COP.
        if (Math.random() < 0.3) {
            monto =
                Math.floor(Math.random() * 1200000) + 800001;
        } else {
            // Montos normales: de $10.000 a $790.000.
            monto =
                (Math.floor(Math.random() * 79) + 1) * 10000;
        }
    }

    return {
        // Nombre escrito por el usuario en el dashboard.
        usuario: inputUsuarioAutomatico.value.trim(),

        atm_id: cajeros[
            Math.floor(Math.random() * cajeros.length)
        ],

        transaction_type: tipo,
        amount: monto
    };
}

function esperar(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}


// ============================================================
// 11. INICIAR SIMULADOR AUTOMÁTICO
// ============================================================

async function ejecutarSimuladorAutomatico() {
    while (envioAutomaticoActivo) {
        const datos = generarTransaccionAutomatica();

        totalEnviadas++;
        actualizarContadores();

        try {
            await enviarTransaccion(datos);

            if (estadoAutomatico) {
                estadoAutomatico.textContent =
                    `Usuario: ${datos.usuario} | ` +
                    `Cajero: ${datos.atm_id} | ` +
                    `Tipo: ${datos.transaction_type} | ` +
                    `Monto: ${formatoCOP.format(datos.amount)}`;
            }

        } catch (error) {
            totalErrores++;
            actualizarContadores();

            if (estadoAutomatico) {
                estadoAutomatico.textContent =
                    `Error para ${datos.usuario}: ${error.message}`;
            }

            console.error(
                "Error del simulador automático:",
                error
            );

        }

        // Una transacción aproximadamente cada segundo.
        if (envioAutomaticoActivo) {
            await esperar(0);
        }
    }

    // Reactivar los controles al finalizar.
    if (btnIniciarAutomatico) {
        btnIniciarAutomatico.disabled = false;
    }

    if (btnDetenerAutomatico) {
        btnDetenerAutomatico.disabled = true;
    }

    if (btnEnviar) {
        btnEnviar.disabled = false;
    }

    if (estadoAutomatico) {
        estadoAutomatico.textContent =
            "Simulador detenido.";
    }
}

if (btnIniciarAutomatico) {
    btnIniciarAutomatico.addEventListener("click", function () {
        if (envioAutomaticoActivo) return;

        const nombreUsuario =
            inputUsuarioAutomatico.value.trim();

        if (!nombreUsuario) {
            estadoAutomatico.textContent =
                "Escribe el nombre del usuario antes de iniciar.";

            inputUsuarioAutomatico.focus();
            return;
        }

        if (envioManualEnCurso) {
            estadoAutomatico.textContent =
                "Espera a que termine el envío manual.";

            return;
        }

        envioAutomaticoActivo = true;

        btnIniciarAutomatico.disabled = true;
        btnDetenerAutomatico.disabled = false;

        if (btnEnviar) {
            btnEnviar.disabled = true;
        }

        estadoAutomatico.textContent =
            `Iniciando simulador para el usuario: ${nombreUsuario}`;

        ejecutarSimuladorAutomatico();
    });
}


// ============================================================
// 12. DETENER SIMULADOR AUTOMÁTICO
// ============================================================

if (btnDetenerAutomatico) {
    btnDetenerAutomatico.addEventListener("click", function () {
        envioAutomaticoActivo = false;
        btnDetenerAutomatico.disabled = true;

        if (estadoAutomatico) {
            estadoAutomatico.textContent =
                "Deteniendo simulador. Se completará la solicitud en curso.";
        }
    });
}


// ============================================================
// 13. CARGAR HISTORIAL DESDE MONGODB
// ============================================================

async function cargarHistorial() {
    if (!tablaHistorial) return;

    const respuesta = await fetch(
        "/api/transacciones",
        { cache: "no-store" }
    );

    if (!respuesta.ok) {
        throw new Error(
            "No se pudo consultar el historial."
        );
    }

    const resultado = await respuesta.json();

    const transacciones = Array.isArray(resultado)
        ? resultado
        : resultado.transacciones || resultado.data || [];

    tablaHistorial.replaceChildren();

    if (transacciones.length === 0) {
        mostrarFilaVacia(
            tablaHistorial,
            6,
            "No hay transacciones almacenadas."
        );

        if (estadoHistorial) {
            estadoHistorial.textContent =
                "No hay registros en MongoDB.";
        }

        return;
    }

    transacciones.forEach(transaccion => {
        const datos = normalizarTransaccion(transaccion);
        const fila = document.createElement("tr");

        fila.appendChild(
            crearCelda(formatearFecha(datos.fecha))
        );
        fila.appendChild(crearCelda(datos.id));
        fila.appendChild(crearCelda(datos.atm));
        fila.appendChild(crearCelda(datos.tipo));
        fila.appendChild(
            crearCelda(formatoCOP.format(datos.monto))
        );
        fila.appendChild(crearCelda(datos.usuario));

        tablaHistorial.appendChild(fila);
    });

    if (estadoHistorial) {
        estadoHistorial.textContent =
            `Se encontraron ${transacciones.length} transacciones en MongoDB.`;
    }
}


// ============================================================
// 14. CARGAR ALERTAS DESDE MONGODB
// ============================================================

async function cargarAlertas() {
    if (!tablaAlertas) return;

    const respuesta = await fetch(
        "/api/alertas",
        { cache: "no-store" }
    );

    if (!respuesta.ok) {
        throw new Error(
            "No se pudieron consultar las alertas."
        );
    }

    const resultado = await respuesta.json();

    const alertas = Array.isArray(resultado)
        ? resultado
        : resultado.alertas || resultado.data || [];

    tablaAlertas.replaceChildren();

    if (totalAlertasElemento) {
        totalAlertasElemento.textContent = alertas.length;
    }

    if (alertas.length === 0) {
        mostrarFilaVacia(
            tablaAlertas,
            6,
            "No hay alertas registradas."
        );

        return;
    }

    alertas.forEach(alerta => {
        const datos = normalizarTransaccion(alerta);

        const fecha = obtenerCampo(
            alerta,
            [
                "fecha_deteccion",
                "fecha_procesamiento",
                "fecha"
            ],
            ""
        );

        const motivo = obtenerCampo(
            alerta,
            [
                "motivo",
                "razon",
                "descripcion",
                "mensaje"
            ],
            "Monto superior al límite configurado"
        );

        const fila = document.createElement("tr");

        fila.appendChild(
            crearCelda(formatearFecha(fecha))
        );
        fila.appendChild(crearCelda(datos.id));
        fila.appendChild(crearCelda(datos.atm));
        fila.appendChild(crearCelda(datos.tipo));
        fila.appendChild(
            crearCelda(formatoCOP.format(datos.monto))
        );
        fila.appendChild(crearCelda(motivo));

        tablaAlertas.appendChild(fila);
    });
}


// ============================================================
// 15. CARGAR ÚLTIMOS ENVÍOS DESDE MONGODB
// ============================================================

async function cargarUltimosEnvios() {
    if (!tablaTransacciones) return;

    const respuesta = await fetch(
        "/api/transacciones",
        { cache: "no-store" }
    );

    if (!respuesta.ok) {
        throw new Error(
            "No se pudieron cargar las transacciones recientes."
        );
    }

    const resultado = await respuesta.json();

    const transacciones = Array.isArray(resultado)
        ? resultado
        : resultado.transacciones || resultado.data || [];

    tablaTransacciones.replaceChildren();

    if (transacciones.length === 0) {
        mostrarFilaVacia(
            tablaTransacciones,
            5,
            "No hay transacciones recientes."
        );

        return;
    }

    transacciones.slice(0, 10).forEach(transaccion => {
        const datos = normalizarTransaccion(transaccion);
        const fila = document.createElement("tr");

        fila.appendChild(
            crearCelda(formatearFecha(datos.fecha))
        );
        fila.appendChild(crearCelda(datos.atm));
        fila.appendChild(crearCelda(datos.tipo));
        fila.appendChild(
            crearCelda(formatoCOP.format(datos.monto))
        );
        fila.appendChild(crearCelda(datos.usuario));

        tablaTransacciones.appendChild(fila);
    });
}


// ============================================================
// 16. ACTUALIZAR TODO EL DASHBOARD
// ============================================================

async function actualizarDashboard() {
    if (actualizandoDatos) return;

    actualizandoDatos = true;

    try {
        const resultados = await Promise.allSettled([
            cargarHistorial(),
            cargarAlertas(),
            cargarUltimosEnvios()
        ]);

        resultados.forEach((resultado, indice) => {
            if (resultado.status === "rejected") {
                console.error(
                    "Error al actualizar el dashboard:",
                    resultado.reason
                );

                if (indice === 0 && estadoHistorial) {
                    estadoHistorial.textContent =
                        "No se pudo cargar el historial. Verifica Flask y MongoDB.";
                }
            }
        });

    } finally {
        actualizandoDatos = false;
    }
}


// ============================================================
// 17. INICIALIZAR EL DASHBOARD
// ============================================================

actualizarFechaActual();
actualizarContadores();
actualizarDashboard();

// Refrescar historial y alertas cada cinco segundos.
setInterval(actualizarDashboard, 5000);

// Actualizar la fecha del encabezado cada minuto.
setInterval(actualizarFechaActual, 60000);
