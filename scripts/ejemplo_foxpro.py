import os
import pandas as pd
from dbfread import DBF

def leer_tabla_foxpro(ruta_archivo):
    """
    Lee una tabla FoxPro (.dbf) y muestra los primeros registros.
    """
    print(f"Intentando leer el archivo: {ruta_archivo}")

    if not os.path.exists(ruta_archivo):
        print(f"Error: El archivo '{ruta_archivo}' no existe.")
        print("Asegúrate de que la ruta sea correcta o proporciona una ruta válida de SIAF.")
        return

    try:
        # Leer el archivo DBF
        # Se usa encoding='latin1' ya que es común en SIAF y char_decode_errors='ignore'
        # para evitar fallos por caracteres extraños.
        tabla_dbf = DBF(ruta_archivo, encoding='latin1', char_decode_errors='ignore')

        # Convertir a un DataFrame de pandas para facilitar la manipulación y visualización
        df = pd.DataFrame(iter(tabla_dbf))

        print("\n¡Lectura exitosa!")
        print(f"La tabla tiene {len(df)} registros y {len(df.columns)} columnas.\n")

        print("Primeros 5 registros:")
        print(df.head())

    except Exception as e:
        print(f"Ocurrió un error al leer la tabla: {e}")

if __name__ == "__main__":
    print("--- Ejemplo de conexión a base de datos FoxPro (DBF) del SIAF ---")
    # Ruta de ejemplo. Cambia esto a la ruta real de tu archivo SIAF, por ejemplo:
    # ruta_ejemplo = "/mnt/siaf_data/DATA/EXPEDIENTE.DBF"

    # Aquí puedes proporcionar la ruta a un archivo DBF de prueba si tienes uno,
    # por ahora usaremos una ruta ficticia para demostrar el manejo de errores.
    ruta_ejemplo = "archivo_prueba.dbf"

    leer_tabla_foxpro(ruta_ejemplo)
