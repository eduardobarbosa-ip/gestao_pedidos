# limpeza_base.py

import sqlite3
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv # ALTERAÇÃO: Importado para carregar variáveis de ambiente

# Deleta pedidos que:
#    - O campo status_processo é igual a 'COMPLETO'.
#    - A data em update_date_delivered é igual ou anterior à data de corte (data atual)

# ==============================================================================
# --- CONFIGURAÇÕES GERAIS E CONSTANTES ---
# ==============================================================================
# ALTERAÇÃO: Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# --- BANCO DE DADOS ---
# ALTERAÇÃO: Busca o caminho do DB da variável de ambiente
DB_FILE = os.getenv('DB_FILE_PATH')

# Verificação para garantir que a variável foi carregada
if not DB_FILE:
    raise ValueError("Erro: A variável de ambiente DB_FILE_PATH deve ser definida.")

# --- FUSO HORÁRIO ---
tz_brasilia = ZoneInfo("America/Sao_Paulo")


# ==============================================================================
# --- FUNÇÃO DE LIMPEZA ---
# ==============================================================================

def limpar_pedidos_antigos():
    """
    Deleta pedidos da base de dados que foram concluídos e, em seguida,
    reorganiza o arquivo do banco de dados.
    """
    conn = None
    # Garante que o diretório para o DB exista, caso contrário, a conexão falhará
    db_dir = os.path.dirname(DB_FILE)
    if db_dir and not os.path.exists(db_dir):
        print(f"INFO: O diretório do banco de dados '{db_dir}' não existe. Nada a limpar.")
        return
        
    if not os.path.exists(DB_FILE):
        print(f"INFO: O arquivo do banco de dados '{DB_FILE}' não foi encontrado. Nada a limpar.")
        return

    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        # 1. Calcular a data de corte (data de hoje)
        hoje = datetime.now(tz_brasilia).date()
        data_corte_str = hoje.isoformat()

        print(f"INFO: A data de corte para exclusão é: {data_corte_str}")
        print("INFO: Pedidos completos entregues nesta data ou antes serão removidos.")

        # 2. Executar o comando DELETE com base nos critérios
        cursor.execute(
            """
            DELETE FROM pedidos
            WHERE
                status_processo = 'COMPLETO'
                AND date(update_date_delivered) <= ?
            """,
            (data_corte_str,)
        )

        registros_deletados = cursor.rowcount
        
        # 3. Confirma a transação de DELETE (mesmo que nada tenha sido deletado)
        conn.commit()

        if registros_deletados > 0:
            print(f"\nSUCESSO: {registros_deletados} pedido(s) antigo(s) foram removidos do banco de dados.")
        else:
            print("\nINFO: Nenhum pedido antigo correspondeu aos critérios para limpeza.")

        # --- Executa o VACUUM incondicionalmente ---
        print("\nINFO: Reorganizando o banco de dados para otimizar o arquivo...")
        cursor.execute("VACUUM")
        conn.commit() # VACUUM precisa de um commit em algumas configurações
        print("INFO: Reorganização concluída.")

    except sqlite3.Error as e:
        print(f"\nERRO: Ocorreu um erro no banco de dados: {e}")
        if conn:
            conn.rollback() # Desfaz a transação em caso de erro
    except Exception as e:
        print(f"\nERRO: Ocorreu um erro inesperado: {e}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    print("======================================================================")
    print("========= SCRIPT DE LIMPEZA DE PEDIDOS ANTIGOS (SQLite) =========")
    print("======================================================================")
    limpar_pedidos_antigos()
    print("\n==================== EXECUÇÃO CONCLUÍDA ====================")