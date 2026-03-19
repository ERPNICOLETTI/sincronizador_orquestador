import os
import time
import sqlite3
import datetime
import dbf
import sys
import subprocess
import logging

# Configurar log para no perder rastro de fallos
logging.basicConfig(
    filename='sincronizador.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ==============================================================================
# CONFIGURACIÓN (RUTAS FIJAS)
# ==============================================================================

# 1. RUTA DE LA BASE DE DATOS (Tu ruta exacta del Escritorio)
DB_SQL_PATH = r"C:\Users\Usuario\Desktop\ERP-PINO\Programa Stock\pickeo.db"

# 2. RUTA DEL DBF (Donde lee Clipper)
RUTA_DESTINO_DBF = r"\\servidor\sistema\VENTAS\MOVSTK"

# 3. Validaciones y Archivos derivados
if not os.path.exists(RUTA_DESTINO_DBF):
    try:
        os.makedirs(RUTA_DESTINO_DBF)
    except:
        pass

DBF_PATH = os.path.join(RUTA_DESTINO_DBF, "NOVEDADES.DBF")
LOCK_FILE = os.path.join(RUTA_DESTINO_DBF, "ocupado.lock")

# Guardamos el contador de ordenes junto a la base de datos para no perderlo
DIR_PROYECTO = os.path.dirname(DB_SQL_PATH)
ORDEN_FILE = os.path.join(DIR_PROYECTO, "orden_actual.txt")

# Estructura para crear el DBF si falta (Compatible con Clipper)
DEF_ESTRUCTURA = "INVCOD C(20); CLIENTE C(20); FECHA D; ORDEN C(20); REMITO C(17); TIPO C(15); CANT N(12,2); INVPEN N(12,2); INVACT N(12,2); ESTADO N(1,0)"

# ==============================================================================
# FUNCIONES AUXILIARES
# ==============================================================================

def limpiar_texto(texto):
    if not texto: return ""
    return str(texto).strip().upper()

def obtener_siguiente_orden():
    """Genera un número secuencial único para el campo ORDEN del DBF"""
    numero = 0
    if os.path.exists(ORDEN_FILE):
        try:
            with open(ORDEN_FILE, "r") as f:
                c = f.read().strip()
                if c.isdigit(): numero = int(c)
        except: pass
    
    numero += 1
    
    try:
        with open(ORDEN_FILE, "w") as f: f.write(str(numero))
    except: pass
    
    return str(numero).zfill(8)

def obtener_codigo_entidad(conn, nombre_entidad):
    """Traduce nombres a códigos. Maneja la lógica del Genérico (*)"""
    nombre = limpiar_texto(nombre_entidad)
    
    if "GENERICO" in nombre or "INGRESO STOCK" in nombre: return "*"
    if nombre in ("ML", "MELI", "MERCADOLIBRE"): return "ML"
    
    cursor = conn.cursor()
    # Buscar en Clientes
    cursor.execute("SELECT codigo FROM cliente WHERE upper(nombre) = ?", (nombre,))
    res = cursor.fetchone()
    if res: return res[0]
    
    # Buscar en Proveedores
    cursor.execute("SELECT codigo FROM proveedor WHERE upper(nombre) = ?", (nombre,))
    res = cursor.fetchone()
    if res: return res[0]
    
    return nombre[:20]

def obtener_pendientes(conn, incluir_ml=False):
    """Filtra qué movimientos procesar según si es modo rápido o modo lote"""
    cursor = conn.cursor()
    
    # Traemos todo lo que NO fue exportado (0 o NULL)
    sql = """
        SELECT m.id, m.sku, m.cantidad, m.fecha, m.origen_stock, m.subtipo,
       o.cliente_nombre, o.origen, o.destino, o.numero_orden, o.tipo_orden
        FROM movimiento m
        LEFT JOIN orden o ON m.orden_id = o.id
        WHERE (m.exportado IS NULL OR m.exportado = 0)
    """
    
    # Si NO estamos en modo Full, ignoramos MercadoLibre
    if not incluir_ml:
        sql += " AND o.origen NOT IN ('MELI', 'ML', 'MERCADOLIBRE')"
    
    sql += " ORDER BY o.cliente_nombre, m.id"
    cursor.execute(sql)
    return cursor.fetchall()

# ==============================================================================
# PROCESO PRINCIPAL
# ==============================================================================

def sincronizar(modo_lote_ml=False):
    print(f"--- Sincronización (Modo ML: {modo_lote_ml}) ---")
    print(f"Base SQL: {DB_SQL_PATH}")
    print(f"Destino DBF: {DBF_PATH}")

    # 1. Validar Base SQL
    if not os.path.exists(DB_SQL_PATH): 
        print(f"ERROR CRITICO: No encuentro la base de datos en: {DB_SQL_PATH}")
        return

    # 2. Validar Semáforo (Lock) con Espera Inteligente
    intentos = 0
    max_intentos = 15  # 15 reintentos * 2 seg = 30 segundos totales
    while os.path.exists(LOCK_FILE) and intentos < max_intentos:
        print(f"DBF Ocupado por otro proceso. Reintentando en 2 seg... ({intentos + 1}/{max_intentos})")
        time.sleep(2)
        intentos += 1

    if os.path.exists(LOCK_FILE):
        print("ERROR: El DBF sigue ocupado después de esperar 30s. Abortando para evitar daños.")
        return

    # 3. Poner CANDADO (Ahora lo ponemos ANTES de cualquier operación)
    try:
        with open(LOCK_FILE, 'w') as f: f.write("LOCKED")
    except Exception as e:
        print(f"No se pudo crear el archivo lock: {e}")
        return

    conn = sqlite3.connect(DB_SQL_PATH)
    conn.row_factory = sqlite3.Row 
    
    try:
        # 4. Buscar pendientes en SQLite
        pendientes = obtener_pendientes(conn, incluir_ml=modo_lote_ml)
        
        if not pendientes:
            msg = "Nada nuevo en SQLite. Ejecutando Orquestador para limpieza..."
            print(msg)
            logging.info(msg)
            try:
                ruta_orquestador = r"\\servidor\sistema\VENTAS\MOVSTK\ORCHESTRATOR.PY"
                if os.path.exists(ruta_orquestador):
                    subprocess.run([sys.executable, ruta_orquestador])
            except:
                pass
            return # El lock se quita en el 'finally'

        msg = f"Iniciando proceso: {len(pendientes)} movimientos detectados."
        print(msg)
        logging.info(msg)
        
        # 5. Abrir o Crear DBF
        if not os.path.exists(DBF_PATH):
            print("El archivo NOVEDADES.DBF no existe. Creándolo...")
            table = dbf.Table(DBF_PATH, DEF_ESTRUCTURA, codepage='cp850')
            table.open(mode=dbf.READ_WRITE)
        else:
            table = dbf.Table(DBF_PATH, codepage='cp850')
            table.open(mode=dbf.READ_WRITE)
        
        ids_procesados = []
        lote_id = datetime.datetime.now().strftime("LOTE_%Y%m%d_%H%M%S")

       # ============================================================
        # 6. AGRUPAR MOVIMIENTOS POR (ORDEN, SKU, ID_MOV)
        # ============================================================

        ids_para_actualizar = []
        fecha_txt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        agrupados = {}

        for row in pendientes:
            origen = (row["origen"] or "").upper()
            if "TRANSFERENCIA" in origen or "REPOSICION" in origen:
                key = (row["numero_orden"], row["sku"])
            else:
                key = (row["numero_orden"], row["sku"], row["id"])


            if key not in agrupados:
                agrupados[key] = {
                    "rows": [],
                    "origen": row["origen"],
                    "destino": row["destino"],
                    "cliente": row["cliente_nombre"],
                }
            agrupados[key]["rows"].append(row)

        # ============================================================
        # 7. ESCRIBIR DBF AGRUPADO
        # ============================================================

        for key, data in agrupados.items():
            numero_orden = key[0]
            sku = key[1]

            try:
                rows = data["rows"]
                origen_upper = (data["origen"] or "").upper()
                destino = (data["destino"] or "DEPO_A_SALON").upper()
                cliente = data["cliente"]

                if "TRANSFERENCIA" in origen_upper or "REPOSICION" in origen_upper:
                    cant_total = abs(float(rows[0]["cantidad"]))
                else:
                    cant_total = sum(float(r["cantidad"]) for r in rows)


                invpen = 0
                invact = 0
                tipo_dbf = origen_upper[:15]
                cod_cli = obtener_codigo_entidad(conn, cliente)

                # --- FILTRO MANUAL / TN (Excepto Cambios) ---
                if any(x in origen_upper for x in ["MANUAL", "TN"]) and rows[0]["tipo_orden"] != "CAMBIO":
                    for r in rows:
                        ids_procesados.append(r["id"])
                    continue

                # --- CAMBIOS ---
                if rows[0]["tipo_orden"] == "CAMBIO":
                    cod_cli = obtener_codigo_entidad(conn, cliente)
                    
                    if rows[0]["subtipo"] == "INGRESO_CAMBIO":
                        # El cliente devuelve -> SUMA stock
                        tipo_dbf = "INGRESO"
                        invpen = float(abs(rows[0]["cantidad"])) if rows[0]["origen_stock"] == "DEPO" else 0
                        invact = float(abs(rows[0]["cantidad"])) if rows[0]["origen_stock"] == "SALON" else 0
                    else:
                        # Reemplazo -> RESTA stock (EGRESO_CAMBIO)
                        tipo_dbf = "EGRESO"
                        invpen = -float(abs(rows[0]["cantidad"])) if rows[0]["origen_stock"] == "DEPO" else 0
                        invact = -float(abs(rows[0]["cantidad"])) if rows[0]["origen_stock"] == "SALON" else 0

                # --- FULL ---
                elif "FULL" in origen_upper:
                    cod_cli = "FULL"
                    tipo_dbf = "EGRESO"
                    for r in rows:
                        if r["origen_stock"] == "SALON":
                            invact -= float(r["cantidad"])
                        else:
                            invpen -= float(r["cantidad"])

                # --- ML ---
                elif any(x in origen_upper for x in ["ML", "MELI", "MERCADOLIBRE"]):
                    cod_cli = "ML"
                    tipo_dbf = "EGRESO"
                    for r in rows:
                        if r["origen_stock"] == "SALON":
                            invact -= float(r["cantidad"])
                        else:
                            invpen -= float(r["cantidad"])

                # --- TRANSFERENCIA O REPOSICION ---
                elif "TRANSFERENCIA" in origen_upper or "REPOSICION" in origen_upper:
                    cod_cli = "INTERNO"
                    tipo_dbf = "TRANSFERENCIA"

                    # Cantidad única (no por filas)
                    cant = abs(float(rows[0]["cantidad"]))

                    if "TRANSFERENCIA" in origen_upper and "SALON_A_DEPO" in destino:
                        # Salón → Depósito
                        invact = -cant
                        invpen = +cant
                    else:
                        # Reposición o Depósito → Salón
                        # Reposición siempre es DEPO a SALON
                        invact = +cant
                        invpen = -cant


                # --- EGRESO normal (cliente '*' u otro) ---
                elif "EGRESO" in origen_upper:
                    tipo_dbf = "EGRESO"
                    for r in rows:
                        if (r["origen_stock"] or "").upper() == "SALON":
                            invact -= float(r["cantidad"])
                        else:
                            invpen -= float(r["cantidad"])

                elif "INGRESO" in origen_upper or "ING_" in origen_upper:
                    tipo_dbf = origen_upper[:15] # Mantenemos el tipo específico (ING_PROD, ING_REV, etc)
                    for r in rows:
                        if (r["origen_stock"] or "").upper() == "SALON":
                            invact += float(r["cantidad"])
                        else:
                            invpen += float(r["cantidad"])
            



                orden_seq = obtener_siguiente_orden()

                table.append((
                    sku[:20],
                    cod_cli[:20],
                    datetime.datetime.now().date(),
                    orden_seq[:20],
                    "",
                    tipo_dbf[:15],
                    cant_total,
                    invpen,
                    invact,
                    0
                ))

                for r in rows:
                    ids_para_actualizar.append((fecha_txt, orden_seq, r["id"]))

            except Exception as e:
                print(f"Error agrupando {sku}: {e}")

        # 7. Confirmar en SQL (Marcar como exportados con el ID numérico)
        if ids_para_actualizar:
            sql = "UPDATE movimiento SET exportado=1, fecha_impacto=?, lote_id=? WHERE id=?"
            conn.executemany(sql, ids_para_actualizar)
            conn.commit()
            print(f"Exito: {len(ids_para_actualizar)} movimientos marcados como exportados.")
        
        # --- 8. AUTOMATIZACIÓN: LLAMAR AL ORQUESTADOR (SIEMPRE, COMO LIMPIEZA) ---
        print(">>> Pasando la posta al ORQUESTADOR para procesar y limpiar...")
        try:
            ruta_orquestador = r"\\servidor\sistema\VENTAS\MOVSTK\ORCHESTRATOR.PY"
            if os.path.exists(ruta_orquestador):
                subprocess.run([sys.executable, ruta_orquestador], check=True)
            else:
                print(f"Alerta: No encuentro el orquestador en {ruta_orquestador}")
        except Exception as e:
            print(f"Error al llamar al Orquestador: {e}")
            with open("sync_error.log", "a") as f:
                f.write(f"{datetime.datetime.now()}: Error Orquestador: {e}\n")

    except Exception as e:
        error_msg = f"Error CRITICO durante la sincronizacion: {e}"
        print(error_msg)
        logging.error(error_msg)
        
    finally:
        # 9. Limpieza final
        conn.close()
        if os.path.exists(LOCK_FILE):
            try: os.remove(LOCK_FILE)
            except: pass

if __name__ == "__main__":
    # Detectar si se pide procesar MercadoLibre (modo full)
    modo_ml = False
    if len(sys.argv) > 1 and sys.argv[1] in ["--full", "--ml"]:
        modo_ml = True
        
    sincronizar(modo_lote_ml=modo_ml)