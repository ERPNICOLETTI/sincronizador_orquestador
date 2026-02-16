
import os
import dbf
import sys

# Try to find SETART.DBF in parent directory relative to current script
current_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in locals() else os.getcwd()
parent_dir = os.path.dirname(current_dir)

POSSIBLE_PATHS = [
    r"C:\Users\Usuario\Desktop\ERP-PINO\Programa Stock\imports\SETART.DBF"
]

def check_dbf(path):
    print(f"\nChecking: {path}")
    if not os.path.exists(path):
        print("  -> File not found.")
        return

    try:
        table = dbf.Table(path)
        table.open(mode=dbf.READ_ONLY)
        print(f"  -> Opened successfully.")
        print(f"  -> Codepage: {table.codepage}")
        print(f"  -> Fields: {table.field_names}")
        
        print(f"  -> Fields: {table.field_names}")
        
        print("  -> First 20 records:")
        for i, rec in enumerate(table):
            if i >= 20: break
            cod = str(rec.INVCOD).strip()
            nom = rec.INVNOM.strip() if 'INVNOM' in table.field_names else "???"
            print(f"    [{cod}] -> {nom}")
            
        table.close()
    except Exception as e:
        print(f"  -> Error: {e}")

if __name__ == "__main__":
    for p in POSSIBLE_PATHS:
        check_dbf(p)
