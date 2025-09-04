# criar_pedidos_db.py

import sqlite3
import requests
import json
import random
import time
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from faker import Faker
import holidays
from dotenv import load_dotenv

# ==============================================================================
# --- CONFIGURAÇÕES GERAIS E CONSTANTES ---
# ==============================================================================
# Carrega as variáveis de ambiente do arquivo .env
load_dotenv() 

# Busca o caminho do DB e a API Key das variáveis de ambiente
DB_FILE = os.getenv('DB_FILE_PATH')
API_KEY = os.getenv('INTELIPOST_API_KEY')

# Verificação para garantir que as variáveis foram carregadas
if not DB_FILE or not API_KEY:
    raise ValueError("Erro: As variáveis de ambiente DB_FILE_PATH e INTELIPOST_API_KEY devem ser definidas.")

QUOTE_API_URL = 'https://api.intelipost.com.br/api/v1/quote_by_product'
ORDER_API_URL = 'https://api.intelipost.com.br/api/v1/shipment_order'
CEP_LOOKUP_API_URL = 'https://brasilapi.com.br/api/cep/v1/'

# Mapeamento de Centros de Distribuição (CDs)
WAREHOUSES = {
    "01": "06612280",
    "02": "29164140",
    "03": "12209050",
    "04": "88706200"
}

# Lista de CEPs (mantida como no original)
CEPS_VALIDOS_BRASIL = [
    # Acre (AC)
    "69900-062", "69908-750", "69909-794", "69918-094", "69911-364", # Rio Branco (Capital)
    "69930-000", "69931-000", "69932-000", "69935-000", "69940-000", # Interior
    "69925-000", "69980-000", "69945-000", "69934-000", "69970-000",
    "69955-000", "69960-000", "69915-238", "69919-814",
    # Alagoas (AL)
    "57010-366", "57025-000", "57035-230", "57045-000", "57051-565", # Maceió (Capital)
    "57240-000", "57260-000", "57200-000", "57300-355", "57312-250", # Interior
    "57100-000", "57120-000", "57150-000", "57160-000", "57230-000",
    "57270-000", "57275-000", "57350-000", "57400-000",
    # Amapá (AP)
    "68900-073", "68901-251", "68903-419", "68906-815", "68909-020", # Macapá (Capital)
    "68925-000", "68950-000", "68997-000", "68930-000", "68940-000", # Interior
    "68945-000", "68948-000", "68955-000", "68960-000", "68970-000",
    "68975-000", "68980-000", "68990-000", "68995-000",
    # Amazonas (AM)
    "69005-300", "69010-001", "69020-000", "69037-000", "69058-833", # Manaus (Capital)
    "69100-000", "69151-000", "69280-000", "69400-000", "69460-000", # Interior
    "69500-000", "69550-000", "69600-000", "69630-000", "69640-000",
    "69730-000", "69735-000", "69750-000", "69800-000",
    # Bahia (BA)
    "40020-056", "40040-000", "40110-010", "41720-020", "41820-021", # Salvador (Capital)
    "45200-000", "45600-000", "45653-005", "44001-205", "44052-092", # Interior
    "48900-000", "48970-000", "48601-010", "47800-000", "47850-000",
    "46100-000", "46400-000", "46430-000", "46880-000",
    # Ceará (CE)
    "60015-000", "60115-170", "60175-055", "60416-522", "60822-455", # Fortaleza (Capital)
    "62010-000", "62040-350", "63010-000", "63050-245", "63500-000", # Interior
    "63560-000", "63600-000", "63640-000", "63700-000", "63800-000",
    "63860-000", "63870-000", "63900-000", "62320-000",
    # Distrito Federal (DF)
    "70040-906", "70070-150", "70150-900", "70200-001", "70310-500", # Brasília (Capital)
    "71571-000", "71680-350", "71745-701", "71900-100", "72015-535", # Cidades Satélites
    "72300-501", "72430-101", "72502-505", "72710-010", "73010-511",
    "73330-001", "73850-000", "70632-100", "70750-515",
    # Espírito Santo (ES)
    "29010-002", "29015-100", "29055-350", "29090-070", "29045-480", # Vitória (Capital)
    "29101-010", "29146-010", "29160-790", "29176-003", "29216-010", # Interior
    "29300-000", "29500-000", "29560-000", "29600-000", "29645-000",
    "29700-010", "29730-000", "29780-000", "29900-020",
    # Goiás (GO)
    "74013-010", "74083-010", "74215-220", "74672-400", "74885-705", # Goiânia (Capital)
    "75020-010", "75113-130", "75250-000", "75380-000", "75503-010", # Interior
    "75650-000", "75690-000", "75701-010", "75780-000", "75800-011",
    "75830-000", "75901-020", "76300-000", "76600-000",
    # Maranhão (MA)
    "65075-441", "65015-560", "65020-250", "65045-380", "65058-137", # São Luís (Capital)
    "65600-000", "65630-000", "65700-000", "65725-000", "65800-000", # Interior
    "65900-000", "65930-000", "65970-000", "65975-000", "65300-000",
    "65365-000", "65370-000", "65390-000", "65400-000",
    # Mato Grosso (MT)
    "78020-400", "78005-300", "78015-200", "78048-000", "78055-502", # Cuiabá (Capital)
    "78550-000", "78600-000", "78700-000", "78715-200", "78850-000", # Interior
    "78890-000", "78110-000", "78200-000", "78250-000", "78260-000",
    "78275-000", "78280-000", "78300-000", "78360-000",
    # Mato Grosso do Sul (MS)
    "79002-203", "79004-390", "79020-300", "79080-190", "79103-240", # Campo Grande (Capital)
    "79800-000", "79820-010", "79900-000", "79950-000", "79200-000", # Interior
    "79240-000", "79290-000", "79300-000", "79320-130", "79400-000",
    "79490-000", "79540-000", "79560-000", "79601-000",
    # Minas Gerais (MG)
    "30180-110", "30110-005", "30130-141", "30310-000", "31160-370", # Belo Horizonte (Capital)
    "35500-000", "35570-000", "35660-000", "35680-000", "35700-000", # Interior
    "35790-000", "35900-000", "35930-000", "36010-000", "36200-000",
    "36500-000", "36570-000", "36770-000", "37002-000",
    # Pará (PA)
    "66010-000", "66023-700", "66050-000", "66085-024", "66645-003", # Belém (Capital)
    "68500-000", "68509-010", "68515-000", "68550-000", "68555-000", # Interior
    "68600-000", "68625-000", "68700-000", "68725-000", "68740-000",
    "68745-000", "68800-000", "68820-000", "68850-000",
    # Paraíba (PB)
    "58013-120", "58025-500", "58038-101", "58046-525", "58071-000", # João Pessoa (Capital)
    "58400-000", "58410-165", "58415-395", "58100-000", "58200-000", # Interior
    "58220-000", "58225-000", "58250-000", "58290-000", "58305-000",
    "58340-000", "58345-000", "58360-000", "58380-000",
    # Paraná (PR)
    "80010-010", "80060-000", "80530-000", "81030-000", "81530-000", # Curitiba (Capital)
    "86010-000", "86020-001", "86050-460", "87013-000", "87020-025", # Interior
    "87050-050", "87111-000", "87200-000", "87300-005", "87501-030",
    "87701-000", "87820-000", "87900-000", "87970-000",
    # Pernambuco (PE)
    "50030-230", "50070-000", "50100-010", "51020-000", "52020-220", # Recife (Capital)
    "54510-000", "54759-000", "55002-000", "55014-000", "55024-000", # Interior
    "55150-000", "55190-000", "55291-010", "55330-000", "55540-000",
    "55590-000", "55602-005", "55641-001", "55813-010",
    # Piauí (PI)
    "64000-120", "64001-280", "64003-077", "64023-530", "64057-375", # Teresina (Capital)
    "64200-000", "64208-150", "64218-020", "64500-000", "64600-000", # Interior
    "64760-000", "64770-000", "64800-000", "64860-000", "64900-000",
    "64980-000", "64230-000", "64255-000", "64260-000",
    # Rio de Janeiro (RJ)
    "20040-004", "20090-003", "20230-010", "21044-020", "22210-030", # Rio de Janeiro (Capital)
    "28905-000", "28940-000", "28970-000", "28990-000", "28890-000", # Interior
    "28605-010", "28613-001", "28625-000", "28800-000", "28820-000",
    "28860-000", "27110-030", "27210-020", "27310-000",
    # Rio Grande do Norte (RN)
    "59012-300", "59020-000", "59030-000", "59040-000", "59064-740", # Natal (Capital)
    "59600-000", "59610-210", "59619-030", "59628-000", "59633-310", # Interior
    "59500-000", "59515-000", "59550-000", "59570-000", "59585-000",
    "59590-000", "59200-000", "59215-000", "59255-000",
    # Rio Grande do Sul (RS)
    "90010-110", "90020-007", "90040-001", "90410-000", "91130-530", # Porto Alegre (Capital)
    "96200-000", "96205-000", "96211-001", "96400-000", "96501-001", # Interior
    "96600-000", "96700-000", "96745-000", "96810-002", "97010-003",
    "97105-001", "97300-000", "97500-001", "97541-001",
    # Rondônia (RO)
    "76801-054", "76803-888", "76804-121", "76812-321", "76820-880", # Porto Velho (Capital)
    "76900-000", "76907-490", "76908-417", "76913-000", "76916-000", # Interior
    "76920-000", "76930-000", "76940-000", "76950-000", "76960-000",
    "76963-741", "76970-000", "76980-000", "76987-004",
    # Roraima (RR)
    "69301-110", "69303-455", "69304-452", "69305-135", "69306-003", # Boa Vista (Capital)
    "69380-000", "69370-000", "69375-000", "69378-000", "69360-000", # Interior
    "69358-000", "69355-000", "69350-000", "69348-000", "69345-000",
    "69343-000", "69340-000", "69330-000", "69325-000",
    # Santa Catarina (SC)
    "88010-001", "88015-200", "88020-301", "88034-001", "88058-300", # Florianópolis (Capital)
    "89010-000", "89020-001", "89035-000", "89052-000", "89066-001", # Interior
    "89110-001", "89120-000", "89160-000", "89201-000", "89216-201",
    "89228-001", "89251-000", "89280-000", "89500-000",
    # São Paulo (SP)
    "01001-000", "01153-000", "01311-000", "02011-000", "04538-132", # São Paulo (Capital)
    "11010-001", "11045-001", "11060-001", "12209-000", "12227-000", # Interior
    "12245-021", "13010-000", "13083-852", "13201-000", "13400-005",
    "13465-000", "13560-001", "13870-000", "14010-000",
    # Sergipe (SE)
    "49010-020", "49015-320", "49025-100", "49035-500", "49047-040", # Aracaju (Capital)
    "49400-000", "49500-000", "49600-000", "49680-000", "49700-000", # Interior
    "49800-000", "49820-000", "49900-000", "49920-000", "49960-000",
    "49980-000", "49100-000", "49140-000", "49160-000",
    # Tocantins (TO)
    "77001-002", "77006-018", "77015-012", "77020-018", "77024-022", # Palmas (Capital)
    "77405-130", "77410-010", "77415-020", "77423-010", "77425-010", # Interior
    "77500-000", "77600-000", "77650-000", "77700-000", "77760-000",
    "77803-120", "77813-010", "77823-010", "77900-000",
]
fake = Faker('pt_BR')
feriados_br = holidays.BR()
tz_brasilia = ZoneInfo("America/Sao_Paulo")

# ==============================================================================
# --- MÓDULO DE GERENCIAMENTO DO BANCO DE DADOS (SQLite) ---
# ==============================================================================
def conectar_db():
    db_dir = os.path.dirname(DB_FILE)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
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
    print("Banco de dados inicializado com sucesso.")

# ==============================================================================
# --- FUNÇÃO PRINCIPAL DE CRIAÇÃO DE PEDIDOS ---
# ==============================================================================
def criar_novos_pedidos(conn, numero_de_pedidos=250):
    """Realiza a cotação e cria novos pedidos, salvando-os no banco de dados."""
    print(f"\n--- Iniciando criação de {numero_de_pedidos} novos pedidos ---")
    
    # --- FUNÇÕES INTERNAS RESTAURADAS ---
    def adicionar_dias_uteis(data_inicial, dias_uteis):
        dias_adicionados = 0
        data_final = data_inicial
        while dias_adicionados < dias_uteis:
            data_final += timedelta(days=1)
            if data_final.weekday() < 5 and data_final not in feriados_br:
                dias_adicionados += 1
        return data_final

    def buscar_endereco_por_cep(cep):
        try:
            response = requests.get(f"{CEP_LOOKUP_API_URL}{cep}", timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException:
            return None

    def realizar_cotacao(origin_zip_code, cep_destino, peso, largura, altura, comprimento):
        custo_do_produto = round(random.uniform(100.0, 5000.0), 2)
        payload = {
            "destination_zip_code": cep_destino.replace('-', ''),
            "origin_zip_code": origin_zip_code,
            "products": [{"weight": peso, "cost_of_goods": custo_do_produto, "width": largura, "height": altura, "length": comprimento, "quantity": 1}]
        }
        headers = {'Content-Type': 'application/json', 'api-key': API_KEY, 'platform': 'automacao'}
        try:
            response = requests.post(QUOTE_API_URL, headers=headers, data=json.dumps(payload), timeout=30)
            response.raise_for_status()
            resultado = response.json().get("content", {})
            opcoes_entrega = resultado.get("delivery_options")
            if not opcoes_entrega: return None
            opcao = min(opcoes_entrega, key=lambda opt: opt.get("provider_shipping_cost", float('inf')))
            return {"cotacao_id": resultado.get("id"), "delivery_method_id": opcao.get("delivery_method_id"), "prazo_dias_uteis": opcao.get("delivery_estimate_business_days"), "custo_frete": opcao.get("provider_shipping_cost"), "custo_produto": custo_do_produto}
        except Exception as e:
            print(f"ERRO na cotação: {e}")
            return None

    # --- LÓGICA PRINCIPAL RESTAURADA ---
    pedidos_criados_count = 0
    for i in range(numero_de_pedidos):
        print(f"\nProcessando criação {i + 1}/{numero_de_pedidos}...")
        
        warehouse_code = random.choice(list(WAREHOUSES.keys()))
        origin_zip_code = WAREHOUSES[warehouse_code]
        print(f"INFO: Usando CD de origem: Código '{warehouse_code}', CEP '{origin_zip_code}'")
        
        cep_destino = random.choice(CEPS_VALIDOS_BRASIL)
        dados_endereco = buscar_endereco_por_cep(cep_destino)
        
        if not dados_endereco: 
            print(f"Não foi possível obter dados para o CEP de destino {cep_destino}. Pulando.")
            continue

        if not dados_endereco.get('street'):
            rua_ficticia = fake.street_name()
            dados_endereco['street'] = rua_ficticia
            print(f"INFO: Rua não encontrada para CEP geral. Usando valor fictício: '{rua_ficticia}'")
        
        if not dados_endereco.get('neighborhood'):
            bairro_ficticio = fake.bairro()
            dados_endereco['neighborhood'] = bairro_ficticio
            print(f"INFO: Bairro não encontrado para CEP geral. Usando valor fictício: '{bairro_ficticio}'")

        p = {"peso": round(random.uniform(0.1, 50.0), 2), "largura": random.randint(1, 100), "altura": random.randint(1, 100), "comprimento": random.randint(1, 100)}
        
        cotacao = realizar_cotacao(origin_zip_code, cep_destino, **p)
        
        if not cotacao or not all(cotacao.values()): 
            print(f"Não foi possível obter cotação para o CEP {cep_destino}. Pulando.")
            continue

        data_criacao = datetime.now(tz_brasilia)
        order_number = f"PEDIDO-{int(time.time())}"
        
        data_estimada_obj = adicionar_dias_uteis(data_criacao, cotacao["prazo_dias_uteis"])
        data_estimada_ajustada = data_estimada_obj.replace(hour=23, minute=59, second=59)
        
        payload_pedido = {
            "quote_id": cotacao["cotacao_id"], 
            "delivery_method_id": cotacao["delivery_method_id"], 
            "order_number": order_number, 
            "origin_warehouse_code": warehouse_code,
            "sales_channel": "Marketplace", 
            "created": data_criacao.isoformat(timespec='seconds'), 
            "shipped_date": data_criacao.isoformat(timespec='seconds'),
            "end_customer": {"first_name": fake.first_name(), "last_name": fake.last_name(), "email": fake.email(), "phone": fake.msisdn(), "cellphone": fake.msisdn(), "is_company": False, "federal_tax_payer_id": fake.cpf().replace('.', '').replace('-', ''), "shipping_country": "Brasil", "shipping_state": dados_endereco.get("state"), "shipping_city": dados_endereco.get("city"), "shipping_address": dados_endereco.get("street"), "shipping_number": str(random.randint(1, 9999)), "shipping_quarter": dados_endereco.get("neighborhood"), "shipping_zip_code": dados_endereco.get("cep").replace('-', '')},
            "shipment_order_volume_array": [{"shipment_order_volume_number": 1, "volume_type_code": "BOX", "weight": p["peso"], "width": p["largura"], "height": p["altura"], "length": p["comprimento"], "products_quantity": 1, "products_nature": "products", "shipment_order_volume_invoice": {"invoice_series": "1", "invoice_number": str(random.randint(1000, 99999)), "invoice_key": ''.join(random.choices('0123456789', k=44)), "invoice_date": data_criacao.isoformat(timespec='seconds'), "invoice_total_value": str(round(cotacao["custo_produto"] + cotacao["custo_frete"], 2)), "invoice_products_value": str(cotacao["custo_produto"]), "invoice_cfop": "6102"}}],
            "estimated_delivery_date": data_estimada_ajustada.isoformat(timespec='seconds')
        }
        
        headers = {'Content-Type': 'application/json', 'api-key': API_KEY, 'platform': 'automacao'}
        try:
            response = requests.post(ORDER_API_URL, headers=headers, data=json.dumps(payload_pedido), timeout=30)
            response.raise_for_status()
            
            cursor = conn.cursor()
            agora_str = datetime.now(tz_brasilia).isoformat()
            cursor.execute("INSERT OR IGNORE INTO pedidos (order_number, status_processo, data_criacao_db, data_atualizacao_db) VALUES (?, ?, ?, ?)", (order_number, 'CRIADO', agora_str, agora_str))
            conn.commit()
            print(f"SUCESSO: Pedido '{order_number}' criado na API e salvo no banco de dados.")
            pedidos_criados_count += 1
        except Exception as e:
            print(f"ERRO na criação do pedido '{order_number}': {e}")
        time.sleep(2)
        
    print(f"\n--- Processo de criação finalizado: {pedidos_criados_count} novos pedidos foram criados. ---")

if __name__ == "__main__":
    print("======================================================================")
    print("====== SCRIPT DE CRIAÇÃO DE PEDIDOS (VERSÃO SQLite) ======")
    print("======================================================================")
    db_conn = None
    try:
        setup_database()
        db_conn = conectar_db()
        criar_novos_pedidos(db_conn, numero_de_pedidos=250)
    except Exception as e:
        print(f"\nERRO CRÍTICO NA EXECUÇÃO: {e}")
    finally:
        if db_conn:
            db_conn.close()
        print("\n==================== EXECUÇÃO CONCLUÍDA ====================")