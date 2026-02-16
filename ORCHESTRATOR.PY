import os
import subprocess
import sys
import sqlite3
from datetime import datetime
import dbf
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4

# ============================================================
# CONFIGURACIÓN
# ============================================================

BASE_PATH = r"\\servidor\sistema\VENTAS"
MOV_PATH  = os.path.join(BASE_PATH, "MOVSTK")

NOVEDADES_DBF = os.path.join(MOV_PATH, "NOVEDADES.DBF")
SQLITE_DB     = os.path.join(MOV_PATH, "stock_movimientos.db")

EXE_INGRESO = os.path.join(BASE_PATH, "REM_ENTR_BATCH.EXE")
EXE_EGRESO  = os.path.join(BASE_PATH, "REM_SALI_BATCH.EXE")
EXE_INTERNO = os.path.join(BASE_PATH, "REM_INTE_BATCH.EXE")

PDF_DIRS = {
    "EGRESO": os.path.join(MOV_PATH, "EGRESOS"),
    "INGRESO": os.path.join(MOV_PATH, "INGRESOS"),
    "TRANSFERENCIA": os.path.join(MOV_PATH, "TRANSFERENCIAS"),
}


# ============================================================
# FUNCIONES NUEVAS (DETECTAR Y LIMPIAR)
# ============================================================

# ============================================================
# PDF REMITOS (MINIMO, SIN ROMPER FLUJO)
# ============================================================

_SETART_CACHE = None

def _get_nombre_producto(invcod: str) -> str:
    global _SETART_CACHE
    try:
        if _SETART_CACHE is None:
            # ACTUALIZADO: Ruta local fuera del servidor
            setart_path = r"C:\Users\Usuario\Desktop\ERP-PINO\Programa Stock\imports\SETART.DBF"
            _SETART_CACHE = {}

            if not os.path.exists(setart_path):
                return ""

            # ⚠️ SIN codepage forzado (ASCII seguro)
            t = dbf.Table(setart_path)
            t.open(mode=dbf.READ_ONLY)

            fields = [f.upper() for f in t.field_names]

            campo_nombre = None
            for c in ["INVNOM", "NOMBRE", "DESCRIP", "DESCRI", "DESCRIPCIO", "ARTNOM"]:
                if c in fields:
                    campo_nombre = c
                    break

            if not campo_nombre:
                t.close()
                return ""

            for r in t:
                cod_raw = str(r.INVCOD).strip().upper()
                nom_raw = str(getattr(r, campo_nombre)).strip()

                if cod_raw:
                    _SETART_CACHE[cod_raw] = nom_raw

            t.close()

            print("SETART cache cargado:", len(_SETART_CACHE), "productos")

        key = str(invcod).strip().upper()
        return _SETART_CACHE.get(key, "")

    except Exception as e:
        print("ERROR SETART:", e)
        return ""



def _emitir_pdf_remito(tipo: str, remito: str, cliente: str, fecha: str, items: list):
    """
    items: lista de dicts: {invcod, cant, delta_salon, delta_depo, nombre}
    """
    tipo = (tipo or "").strip().upper()
    carpeta = PDF_DIRS.get(tipo, MOV_PATH)
    os.makedirs(carpeta, exist_ok=True)

    safe_remito = (remito or "SIN_REMITO").replace("/", "-").replace("\\", "-").strip()
    out_pdf = os.path.join(carpeta, f"{safe_remito}.pdf")

    c = canvas.Canvas(out_pdf, pagesize=A4)
    w, h = A4
    y = h - 40

    # Encabezado
    c.setFont("Helvetica-Bold", 12)
    c.drawString(40, y, f"{cliente} ............................ {tipo} - Remito {safe_remito}")
    y -= 18

    c.setFont("Helvetica", 10)
    c.drawString(40, y, f"Fecha y hora: {fecha}")
    y -= 22

    # Columnas
    c.setFont("Helvetica-Bold", 10)
    c.drawString(40, y, "Código")
    c.drawString(130, y, "Cant")
    c.drawString(180, y, "Nombre")
    c.drawString(430, y, "SALON")
    c.drawString(500, y, "DEPO")
    y -= 14

    c.setFont("Helvetica", 9)

    for it in items:
        if y < 60:
            c.showPage()
            y = h - 40
            c.setFont("Helvetica", 9)

        invcod = str(it.get("invcod", "")).strip()
        cant = it.get("cant", 0)
        nombre = str(it.get("nombre", "")).strip()
        ds = it.get("delta_salon", 0)
        dd = it.get("delta_depo", 0)

        c.drawString(40, y, invcod)
        c.drawRightString(165, y, str(cant))
        c.drawString(180, y, (nombre[:42] + "…") if len(nombre) > 43 else nombre)
        c.drawRightString(470, y, str(ds))
        c.drawRightString(540, y, str(dd))
        y -= 12

    c.save()


def detectar_tipos_pendientes():
    """Lee el DBF para ver qué tipos de movimientos tienen ESTADO=0."""
    tipos = set()
    try:
        if not os.path.exists(NOVEDADES_DBF): return tipos
        
        # Usamos la librería dbf en modo lectura
        table = dbf.Table(NOVEDADES_DBF)
        table.open(mode=dbf.READ_ONLY)
        
        for rec in table:
            if rec.estado == 0:
                t = str(rec.tipo).strip().upper()
                if t: tipos.add(t)
        
        table.close()
    except Exception as e:
        print(f"⚠️ Error leyendo pendientes: {e}")
        
    return tipos

def procesar_resultados_y_limpiar():
    """Migra los ESTADO=1 a SQL y luego los borra físicamente del DBF."""
    print("\n=== MIGRANDO Y LIMPIANDO ===")

    # Conexión SQL
    conn = sqlite3.connect(SQLITE_DB)
    cur = conn.cursor()
    
    migrados = 0
    remitos_pdf = {}  # key: (tipo, remito, cliente, fecha) -> list(items)

    
    try:
        # Abrimos modo Escritura para poder borrar
        table = dbf.Table(NOVEDADES_DBF, codepage='cp850')
        table.open(mode=dbf.READ_WRITE)

        for record in table:
            if record.estado == 1:
                
                # TRUCO: Usamos la hora actual para que el historial tenga HH:MM:SS
                fecha_con_hora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # Normalizar código PRIMERO
                codigo = str(record.invcod).strip().upper()
                nombre = _get_nombre_producto(codigo) or f"[{codigo}]"

                # INSERT SQL con código normalizado
                cur.execute("""
                    INSERT INTO stock_movimientos_his (
                        fecha,
                        remito,
                        invcod,
                        tipo,
                        cant,
                        invpen,
                        invact,
                        cliente
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    fecha_con_hora,
                    str(record.remito).strip(),
                    codigo, 
                    str(record.tipo).strip(),
                    float(record.cant),
                    float(record.invpen),
                    float(record.invact),
                    str(record.cliente).strip()
                ))


                tipo = str(record.tipo).strip().upper()
                rem = str(record.remito).strip()
                cli = str(record.cliente).strip()

                # Mapeo: SALON = invact, DEPO = invpen
                delta_salon = float(record.invact)
                delta_depo  = float(record.invpen)
        
                k = (tipo, rem, cli)
                remitos_pdf.setdefault(k, []).append({
                    "invcod": codigo,
                    "cant": float(record.cant),
                    "delta_salon": delta_salon,
                    "delta_depo": delta_depo,
                    "nombre": nombre
                })


                # 2. Marcar para borrar (Ahora sí se ejecutará siempre)
                dbf.delete(record)
                migrados += 1
        
        conn.commit()

        fecha_pdf = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Emitir PDFs (1 por remito)
        for (tipo, rem, cli), items in remitos_pdf.items():
            _emitir_pdf_remito(tipo, rem, cli, fecha_pdf, items)

        
        # 3. COMPACTAR (Borrado físico)
        if migrados > 0:
            print(f">>> Migrados {migrados} registros. Compactando DBF...")
            table.pack()
        else:
            print(">>> Nada nuevo procesado.")
            
        table.close()

    except Exception as e:
        print(f"❌ Error procesando DBF: {e}")
        conn.rollback()

    conn.close()

# ============================================================
# EJECUTAR EXE
# ============================================================

def ejecutar_exe(path_exe, nombre):
    print(f"\n>>> Ejecutando {nombre} ...")

    if not os.path.exists(path_exe):
        print(f"!!! ERROR: No existe el ejecutable: {path_exe}")
        return

    r = subprocess.run(
        [path_exe],
        cwd=BASE_PATH,
        capture_output=True,
        text=True
    )

    if r.stdout:
        print(r.stdout.strip())
    if r.stderr:
        print("STDERR:", r.stderr.strip())

    print(f">>> {nombre} finalizado.")

# ============================================================
# MAIN (ORDEN CORREGIDO)
# ============================================================

def main():
    print("=== INICIANDO ORQUESTADOR ===")
    print("Fecha/Hora:", datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

    if not os.path.exists(NOVEDADES_DBF):
        print("ERROR: No existe NOVEDADES.DBF")
        return

    # 1. DETECTAR (Antes de correr nada)
    tipos = detectar_tipos_pendientes()

    if not tipos:
        print("\nNo hay movimientos pendientes.")
        # Ejecutamos limpieza por si quedó basura de una corrida anterior fallida
        procesar_resultados_y_limpiar()
        return

    print("\nTipos pendientes:", ", ".join(sorted(tipos)))

    # 2. EJECUTAR CLIPPER (Procesa y cambia Estado 0 -> 1)
    # Incluimos lógica para ML/MELI/FULL dentro de EGRESO
    if any(t in tipos for t in ["EGRESO", "MELI", "ML", "FULL"]):
        ejecutar_exe(EXE_EGRESO, "EGRESOS")

    if "INGRESO" in tipos:
        ejecutar_exe(EXE_INGRESO, "INGRESOS")

    if "TRANSFERENCIA" in tipos:
        ejecutar_exe(EXE_INTERNO, "INTERNOS")

    # 3. MIGRAR Y LIMPIAR (Ahora sí, guardamos lo que Clipper procesó)
    procesar_resultados_y_limpiar()

    print("\n=== ORQUESTADOR FINALIZADO ===")

if __name__ == "__main__":
    main()