import os
import json
import shutil
import pandas as pd
from dbfread import DBF
from sqlalchemy import create_engine
from datetime import datetime

# --- CONFIGURACIÓN DE CONEXIÓN ---
# Ajusta con tus credenciales de Postgres en Docker
DB_USER = "postgres"
DB_PASS = "tu_password"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "cosmo_erp"

# Ruta donde montaste la red del SIAF (Ejemplo: /mnt/siaf_data)
SIAF_PATH = "/mnt/siaf_data/DATA" 

def cargar_configuracion():
    with open('config_tablas.json', 'r') as f:
        return json.load(f)

def shadow_copy(source_path, dest_dir):
    """Copia el archivo DBF a una carpeta local para evitar bloqueos."""
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    
    file_name = os.path.basename(source_path)
    temp_path = os.path.join(dest_dir, file_name)
    shutil.copy2(source_path, temp_path)
    return temp_path

def procesar_siaf():
    config_data = cargar_configuracion()
    conf = config_data['config']
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    print(f"🚀 Iniciando SiafSync - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    for tabla in config_data['tablas']:
        file_path = os.path.join(SIAF_PATH, f"{tabla.upper()}.DBF")
        
        if not os.path.exists(file_path):
            print(f"⚠️  Saltando {tabla}: Archivo no encontrado en {file_path}")
            continue

        try:
            # 1. Crear copia temporal (Shadow Copy)
            print(f"📦 Copiando {tabla}...")
            local_path = shadow_copy(file_path, conf['temp_dir'])

            # 2. Leer DBF con Pandas
            print(f"📖 Leyendo datos de {tabla}...")
            dbf = DBF(local_path, encoding=conf['encoding'], char_decode_errors='ignore')
            df = pd.DataFrame(iter(dbf))

            # 3. Limpieza básica (quitar espacios en blanco)
            df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

            # 4. Carga a PostgreSQL
            target_table = f"{conf['prefix_table']}{tabla.lower()}"
            print(f"📤 Subiendo a Postgres: {target_table} ({len(df)} registros)")
            
            # Usamos 'replace' para tener la data fresca. 
            # Si prefieres historial, usa 'append' con lógica de duplicados.
            df.to_sql(target_table, engine, if_exists='replace', index=False)
            
            # 5. Limpiar archivo temporal
            os.remove(local_path)
            print(f"✅ {tabla} procesada con éxito.")

        except Exception as e:
            print(f"❌ Error procesando {tabla}: {str(e)}")

    print(f"🏁 Proceso terminado.")

if __name__ == "__main__":
    procesar_siaf()
