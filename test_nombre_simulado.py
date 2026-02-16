import os
import dbf
from datetime import datetime

# Simulación exacta de lo que hace el orquestador
_SETART_CACHE = None

def _get_nombre_producto(invcod: str) -> str:
    global _SETART_CACHE
    try:
        if _SETART_CACHE is None:
            # ACTUALIZADO: Ruta local fuera del servidor
            setart_path = r"C:\Users\Usuario\Desktop\ERP-PINO\Programa Stock\imports\SETART.DBF"
            _SETART_CACHE = {}

            if not os.path.exists(setart_path):
                print(f"ERROR: No se encuentra el archivo {setart_path}")
                return ""

            print(f"Leyendo DBF: {setart_path}")
            
            # ⚠️ SIN codepage forzado (ASCII seguro)
            t = dbf.Table(setart_path)
            t.open(mode=dbf.READ_ONLY)

            fields = [f.name.upper() for f in t.field_names]

            campo_nombre = None
            for c in ["INVNOM", "NOMBRE", "DESCRIP", "DESCRI", "DESCRIPCIO", "ARTNOM"]:
                if c in fields:
                    campo_nombre = c
                    break

            if not campo_nombre:
                print("ERROR: No se detectó campo de nombre")
                t.close()
                return ""

            print(f"Campo nombre detectado: {campo_nombre}")

            for r in t:
                cod_raw = str(r.INVCOD).strip().upper()
                nom_raw = str(getattr(r, campo_nombre)).strip()

                if cod_raw:
                    _SETART_CACHE[cod_raw] = nom_raw

            t.close()

            print("SETART cache cargado:", len(_SETART_CACHE), "productos")

        key = str(invcod).strip().upper()
        if key in _SETART_CACHE:
            return f"ENCONTRADO: {_SETART_CACHE[key]}"
        else:
            return "NO ENCONTRADO"

    except Exception as e:
        print("ERROR SETART:", e)
        return ""

# Prueba ESPECÍFICA con PSMD
codigo_a_probar = "PSMD"
print(f"\n--- PRUEBA DE BÚSQUEDA PARA: {codigo_a_probar} ---")
resultado = _get_nombre_producto(codigo_a_probar)
print(f"Resultado: {resultado}")

# Prueba con un código CON GUIONES para confirmar la corrección
codigo_con_guion = "30-500" # Ejemplo
print(f"\n--- PRUEBA DE BÚSQUEDA PARA: {codigo_con_guion} ---")
resultado_guion = _get_nombre_producto(codigo_con_guion)
print(f"Resultado: {resultado_guion}")
