# gerenciar_status_pedidos_db.py

import sqlite3
import requests
import json
import random
import os
import math
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv # ALTERAÇÃO: Importado para carregar variáveis de ambiente

# ==============================================================================
# --- CONFIGURAÇÕES GERAIS E CONSTANTES ---
# ==============================================================================
# ALTERAÇÃO: Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# ALTERAÇÃO: Busca o caminho do DB e a API Key principal das variáveis de ambiente
DB_FILE = os.getenv('DB_FILE_PATH')
API_KEY = os.getenv('INTELIPOST_API_KEY')

# Verificação para garantir que as variáveis foram carregadas
if not DB_FILE or not API_KEY:
    raise ValueError("Erro: As variáveis de ambiente DB_FILE_PATH e INTELIPOST_API_KEY devem ser definidas.")

try:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    SCRIPT_DIR = os.getcwd()

TRACKING_API_URL = 'https://api.intelipost.com.br/api/v1/tracking/add/events'

# --- AJUSTE: Mapeamento de transportadoras com API keys carregadas do ambiente ---
CARRIER_MAP = {
    "32": {
        "api_key": os.getenv("CARRIER_32_API_KEY"),
        "codes": {"shipped": "98", "in_transit": "98", "to_be_delivered": "31", "delivered": "01"}
    },
    "4": {
        "api_key": os.getenv("CARRIER_4_API_KEY"),
        "codes": {"shipped": "101", "in_transit": "101", "to_be_delivered": "182", "delivered": "01"}
    },
    "177": {
        "api_key": os.getenv("CARRIER_177_API_KEY"),
        "codes": {"shipped": "098", "in_transit": "098", "to_be_delivered": "31", "delivered": "001"}
    },
    "51": {
        "api_key": os.getenv("CARRIER_51_API_KEY"),
        "codes": {"shipped": "18", "in_transit": "18", "to_be_delivered": "31", "delivered": "35"}
    },
    "3363": {
        "api_key": os.getenv("CARRIER_3363_API_KEY"),
        "codes": {"shipped": "98", "in_transit": "98", "to_be_delivered": "31", "delivered": "01"}
    },
    "23": {
        "api_key": os.getenv("CARRIER_23_API_KEY"),
        "codes": {"shipped": "98", "in_transit": "98", "to_be_delivered": "101", "delivered": "01"}
    }
}

# Verificação para as chaves das transportadoras
for carrier_id, data in CARRIER_MAP.items():
    if not data["api_key"]:
        raise ValueError(f"Erro: A variável de ambiente CARRIER_{carrier_id}_API_KEY não foi definida.")

tz_brasilia = ZoneInfo("America/Sao_Paulo")

# ==============================================================================
# --- MÓDULO DE GERENCIAMENTO DO BANCO DE DADOS (SQLite) ---
# ==============================================================================
def conectar_db():
    # Garante que o diretório para o DB exista
    db_dir = os.path.dirname(DB_FILE)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

# ... (o restante do script permanece o mesmo) ...

# ==============================================================================
# --- MÓDULOS DE GERENCIAMENTO DE STATUS ---
# ==============================================================================
# ... (todo o resto do script permanece inalterado) ...

if __name__ == "__main__":
    print("="*80); print("====== SCRIPT DE CONSULTA E GESTÃO DE STATUS DE PEDIDOS (VERSÃO SQLite) ======"); print("="*80)
    db_conn = None
    try:
        setup_database()
        db_conn = conectar_db()
        consultar_pedidos_criados(db_conn)
        marcar_pedidos_para_atraso(db_conn)
        enviar_atualizacoes_de_status(db_conn)
    except Exception as e:
        print(f"\nERRO CRÍTICO NA EXECUÇÃO: {e}")
    finally:
        if db_conn: db_conn.close()
        print("\n==================== EXECUÇÃO CONCLUÍDA ====================")