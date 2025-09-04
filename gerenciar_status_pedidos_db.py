# gerenciar_status_pedidos_db.py

import sqlite3
import requests
import json
import random
import os
import math
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

# ==============================================================================
# --- CONFIGURAÇÕES GERAIS E CONSTANTES ---
# ==============================================================================
# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# --- CONFIGURAÇÕES CARREGADAS DO AMBIENTE ---
DB_FILE = os.getenv('DB_FILE_PATH')
API_KEY = os.getenv('INTELIPOST_API_KEY')

# Verificação para garantir que as variáveis foram carregadas
if not DB_FILE or not API_KEY:
    raise ValueError("Erro: As variáveis de ambiente DB_FILE_PATH e INTELIPOST_API_KEY devem ser definidas.")

TRACKING_API_URL = 'https://api.intelipost.com.br/api/v1/tracking/add/events'

# Mapeamento de transportadoras com API keys carregadas do ambiente
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
    db_dir = os.path.dirname(DB_FILE)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def setup_database():
    """Cria a tabela de pedidos se ela não existir."""
    conn = conectar_db()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pedidos (
            order_number TEXT PRIMARY KEY,
            status_processo TEXT NOT NULL,
            latest_volume_state TEXT,
            created_iso TEXT,
            estimated_delivery_date_iso TEXT,
            delivery_method_id TEXT,
            full_response_json TEXT,
            late_delivery_flag INTEGER NOT NULL DEFAULT 0,
            data_criacao_db TEXT,
            data_atualizacao_db TEXT,
            update_date_in_transit TEXT,
            update_date_to_be_delivered TEXT,
            update_date_delivered TEXT
        )
    ''')
    conn.commit()
    conn.close()

# ==============================================================================
# --- MÓDULOS DE GERENCIAMENTO DE STATUS (LÓGICA RESTAURADA) ---
# ==============================================================================
def consultar_pedidos_criados(conn):
    """Consulta os detalhes de pedidos e calcula e salva as datas de update."""
    print("\n--- ETAPA 1: Iniciando consulta de pedidos com status 'CRIADO' ---")
    cursor = conn.cursor()
    cursor.execute("SELECT order_number FROM pedidos WHERE status_processo = 'CRIADO'")
    pedidos_para_consultar = [row['order_number'] for row in cursor.fetchall()]
    if not pedidos_para_consultar: print("Nenhum pedido novo para consultar."); return
    headers = {'Content-Type': 'application/json', 'api-key': API_KEY, 'platform': 'automacao'}
    
    for order_number in pedidos_para_consultar:
        print(f"Consultando pedido '{order_number}'...")
        try:
            response = requests.get(f"https://api.intelipost.com.br/api/v1/shipment_order/{order_number}", headers=headers, timeout=30)
            response.raise_for_status()
            content = response.json().get("content", {})
            
            latest_state, volume_array = "N/A", content.get("shipment_order_volume_array", [])
            if volume_array: latest_state = volume_array[0].get("shipment_order_volume_state", "N/A")

            data_criacao_str = content.get("created_iso")
            data_estimada_str = content.get("estimated_delivery_date_iso")
            update_dates = {'in_transit': None, 'to_be_delivered': None, 'delivered': None}

            if data_criacao_str and data_estimada_str:
                data_criacao = datetime.fromisoformat(data_criacao_str).date()
                data_estimada = datetime.fromisoformat(data_estimada_str).date()
                total_prazo_dias = max(1, (data_estimada - data_criacao).days)

                t_in_transit = random.uniform(0.15, 0.60)
                t_delivered = random.uniform(0.80, 1.00)

                dias_para_in_transit = math.ceil(total_prazo_dias * t_in_transit)
                dias_para_delivered = math.ceil(total_prazo_dias * t_delivered)

                update_dates['in_transit'] = (data_criacao + timedelta(days=dias_para_in_transit)).isoformat()
                update_dates['delivered'] = (data_criacao + timedelta(days=dias_para_delivered)).isoformat()
                update_dates['to_be_delivered'] = update_dates['delivered']

            cursor.execute(
                """UPDATE pedidos SET 
                   status_processo = ?, latest_volume_state = ?, created_iso = ?, estimated_delivery_date_iso = ?, 
                   delivery_method_id = ?, full_response_json = ?, data_atualizacao_db = ?,
                   update_date_in_transit = ?, update_date_to_be_delivered = ?, update_date_delivered = ?
                   WHERE order_number = ?""",
                ('CONSULTADO', latest_state, content.get("created_iso"), content.get("estimated_delivery_date_iso"), 
                 content.get("delivery_method_id"), json.dumps(response.json()), datetime.now(tz_brasilia).isoformat(),
                 update_dates['in_transit'], update_dates['to_be_delivered'], update_dates['delivered'],
                 order_number)
            )
            conn.commit()
            print(f"SUCESSO: Pedido '{order_number}' consultado. Datas de entrega futuras calculadas e salvas.")
        except Exception as e:
            print(f"ERRO ao consultar o pedido '{order_number}': {e}")

def marcar_pedidos_para_atraso(conn):
    """Marca novos pedidos para atraso (se a cota de 2% não foi atingida) e define sua nova data de entrega."""
    print("\n--- ETAPA 2: Iniciando marcação de pedidos para simular atraso ---")
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM pedidos WHERE status_processo = 'CONSULTADO'")
    total_abertos = cursor.fetchone()[0]
    cursor.execute("SELECT count(*) FROM pedidos WHERE status_processo = 'CONSULTADO' AND late_delivery_flag = 1")
    total_ja_atrasados = cursor.fetchone()[0]
    if total_abertos == 0:
        print("Nenhum pedido em aberto para avaliar."); return
    limite_atraso = int(total_abertos * 0.02)
    print(f"INFO: Total de pedidos abertos: {total_abertos}. Meta de atrasados (2%): {limite_atraso}. Já marcados: {total_ja_atrasados}.")
    if total_ja_atrasados >= limite_atraso:
        print("A cota de 2% de pedidos em atraso já foi atingida ou superada. Nenhum novo pedido será marcado."); return
    num_para_marcar = limite_atraso - total_ja_atrasados
    print(f"Necessário marcar mais {num_para_marcar} pedido(s) para atingir a meta.")
    cursor.execute("SELECT * FROM pedidos WHERE status_processo = 'CONSULTADO' AND late_delivery_flag = 0")
    pedidos_candidatos = cursor.fetchall()
    if not pedidos_candidatos: print("Nenhum pedido elegível (sem flag de atraso) encontrado."); return
    hoje = datetime.now(tz_brasilia).date()
    pedidos_ordenados = []
    for pedido in pedidos_candidatos:
        data_estimada_str = pedido["estimated_delivery_date_iso"]
        if data_estimada_str and data_estimada_str != 'None':
            diferenca_dias = abs((datetime.fromisoformat(data_estimada_str).date() - hoje).days)
            pedidos_ordenados.append({'diff': diferenca_dias, 'order_number': pedido['order_number'], 'est_date_iso': data_estimada_str})
    pedidos_ordenados.sort(key=lambda item: item['diff'])
    pedidos_selecionados = random.sample(pedidos_ordenados, min(num_para_marcar, len(pedidos_ordenados)))
    if not pedidos_selecionados: print("Nenhum pedido selecionado para atraso nesta execução."); return
    pedidos_marcados_list = []
    for pedido in pedidos_selecionados:
        data_estimada = datetime.fromisoformat(pedido['est_date_iso']).date()
        nova_data_entrega = (data_estimada + timedelta(days=1)).isoformat()
        cursor.execute(
            "UPDATE pedidos SET late_delivery_flag = 1, update_date_delivered = ?, update_date_to_be_delivered = ?, data_atualizacao_db = ? WHERE order_number = ?",
            (nova_data_entrega, nova_data_entrega, datetime.now(tz_brasilia).isoformat(), pedido['order_number'])
        )
        pedidos_marcados_list.append(pedido['order_number'])
    conn.commit()
    print(f"SUCESSO: {len(pedidos_marcados_list)} novos pedidos foram marcados para entrega em atraso: {', '.join(pedidos_marcados_list)}")

def enviar_atualizacoes_de_status(conn):
    """Processa pedidos 'CONSULTADOS', comparando a data atual com as datas de update salvas."""
    print("\n--- ETAPA 3: Iniciando envio de eventos de tracking ---")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM pedidos WHERE status_processo = 'CONSULTADO'")
    pedidos_para_processar = cursor.fetchall()
    if not pedidos_para_processar: print("Nenhum pedido no estado 'CONSULTADO' para processar."); return
    
    hoje = datetime.now(tz_brasilia).date()

    for pedido in pedidos_para_processar:
        order_number = pedido['order_number']
        print(f"\nProcessando pedido '{order_number}' com estado '{pedido['latest_volume_state']}'...")
        
        is_late_order = pedido['late_delivery_flag'] == 1
        agora = datetime.now(tz_brasilia)
        latest_volume_state = pedido['latest_volume_state']
        delivery_method_id = str(pedido['delivery_method_id']) # Garante que seja string para a chave do dict

        carrier_info = CARRIER_MAP.get(delivery_method_id)

        if not carrier_info: 
            print(f"AVISO: Delivery method ID '{delivery_method_id}' não mapeado. Pulando.")
            continue
            
        logistic_api_key = carrier_info["api_key"]
        carrier_codes = carrier_info["codes"]

        def enviar_evento(evento):
            payload = {"order_number": order_number, "events": [evento]}
            headers = {'Content-Type': 'application/json', 'logistic-provider-api-key': logistic_api_key, 'platform': 'automacao'}
            print(f"Enviando evento '{evento['original_code']}' para o pedido '{order_number}'...")
            try:
                response = requests.post(TRACKING_API_URL, headers=headers, data=json.dumps(payload), timeout=30)
                response.raise_for_status(); return True
            except Exception as e: print(f"ERRO ao enviar evento: {e}"); return False

        if latest_volume_state == "SHIPPED":
            data_alvo_str = pedido['update_date_in_transit']
            if data_alvo_str and hoje >= (data_alvo := datetime.fromisoformat(data_alvo_str).date()):
                novo_estado, codigo_evento = "IN_TRANSIT", carrier_codes["in_transit"]
                
                if hoje > data_alvo:
                    print(f"INFO: Processando evento de {data_alvo} com atraso.")
                    event_ts = datetime.combine(data_alvo, datetime.min.time()).replace(hour=23, minute=57, tzinfo=tz_brasilia)
                else:
                    event_ts = agora - timedelta(hours=3)

                if enviar_evento({"event_date": event_ts.isoformat(timespec='seconds'), "original_code": codigo_evento}):
                    cursor.execute("UPDATE pedidos SET latest_volume_state = ?, data_atualizacao_db = ? WHERE order_number = ?", (novo_estado, agora.isoformat(), order_number)); conn.commit()
                    print(f"Estado do pedido '{order_number}' atualizado para '{novo_estado}'.")
            else: 
                print(f"INFO: Aguardando data planejada para mover para 'IN_TRANSIT' ({data_alvo_str}).")
        
        elif latest_volume_state == "IN_TRANSIT":
            if is_late_order:
                data_alvo_str = pedido['update_date_to_be_delivered']
                if data_alvo_str and hoje >= (data_alvo := datetime.fromisoformat(data_alvo_str).date()):
                    novo_estado, codigo_evento = "TO_BE_DELIVERED", carrier_codes["to_be_delivered"]
                    if hoje > data_alvo:
                        event_ts = datetime.combine(data_alvo, datetime.min.time()).replace(hour=23, minute=58, tzinfo=tz_brasilia)
                    else:
                        event_ts = agora - timedelta(hours=2)
                    if enviar_evento({"event_date": event_ts.isoformat(timespec='seconds'), "original_code": codigo_evento}):
                        cursor.execute("UPDATE pedidos SET latest_volume_state = ?, data_atualizacao_db = ? WHERE order_number = ?", (novo_estado, agora.isoformat(), order_number)); conn.commit()
                        print(f"Estado do pedido '{order_number}' atualizado para '{novo_estado}'.")
                else: 
                    print(f"INFO: Pedido em atraso aguardando data planejada para 'TO_BE_DELIVERED' ({data_alvo_str}).")
            else:
                data_alvo_str = pedido['update_date_delivered']
                if data_alvo_str and hoje >= (data_alvo := datetime.fromisoformat(data_alvo_str).date()):
                    print("INFO: Pedido no prazo. Data alvo atingida. Enviando eventos finais...")
                    codigo_em_rota, codigo_entregue = carrier_codes["to_be_delivered"], carrier_codes["delivered"]
                    
                    if hoje > data_alvo:
                        event_ts_em_rota = datetime.combine(data_alvo, datetime.min.time()).replace(hour=23, minute=58, tzinfo=tz_brasilia)
                        event_ts_entregue = datetime.combine(data_alvo, datetime.min.time()).replace(hour=23, minute=59, tzinfo=tz_brasilia)
                    else:
                        event_ts_em_rota = agora - timedelta(hours=2)
                        event_ts_entregue = agora - timedelta(hours=1)

                    if enviar_evento({"event_date": event_ts_em_rota.isoformat(timespec='seconds'), "original_code": codigo_em_rota}) and \
                       enviar_evento({"event_date": event_ts_entregue.isoformat(timespec='seconds'), "original_code": codigo_entregue}):
                        cursor.execute("UPDATE pedidos SET status_processo = ?, latest_volume_state = ?, data_atualizacao_db = ? WHERE order_number = ?", ('COMPLETO', 'DELIVERED', agora.isoformat(), order_number)); conn.commit()
                        print(f"SUCESSO: Pedido '{order_number}' finalizado e movido para 'COMPLETO'.")
                else: 
                    print(f"INFO: Aguardando data planejada para eventos finais ({data_alvo_str}).")

        elif latest_volume_state == "TO_BE_DELIVERED":
            data_alvo_str = pedido['update_date_delivered']
            if data_alvo_str and hoje >= (data_alvo := datetime.fromisoformat(data_alvo_str).date()):
                codigo_entregue = carrier_codes["delivered"]
                
                if hoje > data_alvo:
                    event_ts = datetime.combine(data_alvo, datetime.min.time()).replace(hour=23, minute=59, tzinfo=tz_brasilia)
                else:
                    event_ts = agora - timedelta(hours=1)
                
                if enviar_evento({"event_date": event_ts.isoformat(timespec='seconds'), "original_code": codigo_entregue}):
                    cursor.execute("UPDATE pedidos SET status_processo = ?, latest_volume_state = ?, data_atualizacao_db = ? WHERE order_number = ?", ('COMPLETO', 'DELIVERED', agora.isoformat(), order_number)); conn.commit()
                    print(f"SUCESSO: Pedido '{order_number}' finalizado e movido para 'COMPLETO'.")
            else:
                print(f"INFO: Aguardando data planejada para finalizar entrega ({data_alvo_str}).")
        else:
            print(f"AVISO: Nenhuma ação definida para o estado '{latest_volume_state}'.")

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