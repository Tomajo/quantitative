import sys
sys.path.append("py")
import requests
import pandas as pd
import mysql.connector
import io
import requests
from get_key import get_key

# Clave API de EODHD

API_KEY = get_key()


# Base de datos MySQL
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "B4yesian",
    "database": "quantitative_db"
}

# Obtener lista de bolsas disponibles en EODHD
def get_available_exchanges():
    """Descarga la lista de bolsas disponibles en EODHD."""
    url = f"https://eodhd.com/api/exchanges-list/?api_token={API_KEY}"
    response = requests.get(url)
    
    if response.status_code != 200:
        print("⚠️ No se pudo obtener la lista de bolsas.")
        return []
    
    try:
        exchanges = response.json()
        return [e["Code"] for e in exchanges]  # Extraer códigos de bolsas
    except:
        print("⚠️ Error al procesar la respuesta de bolsas disponibles.")
        return []

# Obtener la lista de mercados que realmente existen
available_exchanges = get_available_exchanges()
print(f"📌 Mercados disponibles en EODHD: {available_exchanges}")

#https://eodhd.com/api/exchanges-list/?api_token=67cdfe5a569862.10435705
# Mercados que queremos descargar
TARGET_EXCHANGES = ['US', 'LSE', 'TO', 'V', 'NEO', 'BE', 'HM', 'XETRA', 'DU', 'HA', 'MU', 'STU', 'F', 'LU', 
                    'VI', 'PA', 'BR', 'MC', 'SW', 'LS', 'AS', 'IC', 'IR', 'HE', 'OL', 'CO', 'ST', 'VFEX', 'XZIM', 
                    'LUSE', 'USE', 'DSE', 'PR', 'RSE', 'XBOT', 'EGX', 'XNSA', 'GSE', 'MSE', 'BRVM', 'XNAI', 'BC',
                    'SEM', 'TA', 'KQ', 'KO', 'BUD', 'WAR', 'PSE', 'JK', 'AU', 'SHG', 'KAR', 'JSE', 'NSE', 'AT', 
                    'SHE', 'SN', 'BK', 'CM', 'VN', 'KLSE', 'RO', 'SA', 'BA', 'MX', 'IL', 'ZSE', 'TW', 'TWO']



# Función para obtener tickers de un mercado
def get_tickers_csv(exchange):
    """Descarga la lista de tickers en formato CSV y la convierte en DataFrame"""
    url = f"https://eodhd.com/api/exchange-symbol-list/{exchange}?api_token={API_KEY}&type=stock&fmt=csv"
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"⚠️ Error en {exchange}: {response.status_code}")
        return pd.DataFrame()
    
    df = pd.read_csv(io.StringIO(response.text))
    df["exchange"] = exchange  # Agregar columna con el mercado
    return df

# Descargar tickers de todas las bolsas
all_tickers = pd.DataFrame()
for exchange in TARGET_EXCHANGES:
    df = get_tickers_csv(exchange)
    if not df.empty:
        all_tickers = pd.concat([all_tickers, df], ignore_index=True)

# Mostrar las columnas disponibles
print("📌 Columnas obtenidas:", all_tickers.columns)

# Ajustar nombres de columnas a la estructura de MySQL
all_tickers.rename(columns={"Code": "symbol", "Name": "name", "Country": "country", "Type": "type","Isin": "isin"}, inplace=True)

# Conectar a MySQL
conn = mysql.connector.connect(**MYSQL_CONFIG)
cursor = conn.cursor()


cursor.execute("""DROP TABLE tickers""")

# Crear tabla si no existe
cursor.execute("""
    CREATE TABLE IF NOT EXISTS tickers (
        symbol VARCHAR(50) PRIMARY KEY,
        name VARCHAR(255),
        exchange VARCHAR(10),
        country VARCHAR(50),
        type VARCHAR(20)
    )
""")

# Insertar datos en MySQL
insert_query = """
    INSERT INTO tickers (symbol, name, exchange, country, type)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE 
    name = VALUES(name), exchange = VALUES(exchange), 
    country = VALUES(country), type = VALUES(type);
"""
#all_tickers = all_tickers.dropna()
all_tickers = all_tickers.dropna(subset=["symbol"])
data = all_tickers[["symbol", "name", "exchange", "country", "type"]].values.tolist()

cursor.executemany(insert_query, data)
conn.commit()

print(f"✅ Se insertaron {cursor.rowcount} tickers en MySQL.")

# Cerrar conexión
cursor.close()
conn.close()


