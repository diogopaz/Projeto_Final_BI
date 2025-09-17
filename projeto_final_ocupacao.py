import pandas as pd
import sqlite3
from datetime import datetime, timezone, timedelta
import sys
import os

def carregar_dados_merge(csv_path, db_path):
    """
    Executa o processo de ETL (Merge) para carregar e atualizar dados de ocupações.
    :param csv_path: Caminho para o arquivo CSV de origem.
    :param db_path: Caminho para o arquivo de banco de dados SQLite.
    """
    # --- 1. LEITURA E TRANSFORMAÇÃO DOS DADOS DE ORIGEM ---
    try:
        df_origem = pd.read_csv(csv_path, encoding='latin-1', sep=';')
        if 'CODIGO' not in df_origem.columns or 'TITULO' not in df_origem.columns:
            print("Erro: O arquivo CSV deve conter as colunas 'CODIGO' e 'TITULO'.")
            return
    except Exception as e:
        print(f"Ocorreu um erro ao ler o arquivo CSV: {e}")
        return

    df_origem.rename(columns={'CODIGO': 'CD_OCUP', 'TITULO': 'DS_OCUP'}, inplace=True)
    br_tz = timezone(timedelta(hours=-3))
    data_carga_atual = datetime.now(br_tz).strftime('%Y-%m-%d %H:%M:%S')
    df_origem['DT_CARGA'] = data_carga_atual
    
    print("--- DataFrame de Origem (Amostra) ---")
    print(df_origem.head())

    # --- 2. LÓGICA DE CARGA MERGE ---
    table_name = "DWCD_OCUP"
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Garante que a tabela exista (cria apenas na primeira vez)
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                "SK_OCUP" INTEGER NOT NULL UNIQUE,
                "CD_OCUP" INTEGER NOT NULL UNIQUE,
                "DS_OCUP" VARCHAR NOT NULL,
                "DT_CARGA" DATETIME,
                PRIMARY KEY("SK_OCUPACAO" AUTOINCREMENT)
            );
            """
        )
        conn.commit()

        # Lê os dados existentes no DW
        df_destino = pd.read_sql(f'SELECT CD_OCUP, DS_OCUP FROM {table_name}', conn)

        # Junta (merge) os dataframes de origem e destino para comparação
        df_merged = pd.merge(
            df_origem,
            df_destino,
            on='CD_OCUP',
            how='left',
            suffixes=('_origem', '_destino')
        )

        # --- INSERTS: Filtra os registros que são novos ---
        df_para_inserir = df_merged[df_merged['DS_OCUP_destino'].isnull()]
        dados_para_inserir = df_para_inserir[['CD_OCUP', 'DS_OCUP_origem', 'DT_CARGA']].to_records(index=False).tolist()

        if dados_para_inserir:
            sql_insert = f'INSERT INTO "{table_name}" (CD_OCUP, DS_OCUP, DT_CARGA) VALUES (?, ?, ?)'
            cursor.executemany(sql_insert, dados_para_inserir)
            print(f"\n{len(dados_para_inserir)} novos registros inseridos.")
        else:
            print("\nNenhum registro novo para inserir.")

        # --- UPDATES: Filtra registros existentes que mudaram ---
        df_para_atualizar = df_merged.dropna(subset=['DS_OCUP_destino'])
        df_para_atualizar = df_para_atualizar[df_para_atualizar['DS_OCUP_origem'] != df_para_atualizar['DS_OCUP_destino']]
        dados_para_atualizar = df_para_atualizar[['DS_OCUP_origem', 'DT_CARGA', 'CD_OCUP']].to_records(index=False).tolist()

        if dados_para_atualizar:
            sql_update = f'UPDATE "{table_name}" SET DS_OCUP = ?, DT_CARGA = ? WHERE CD_OCUP = ?'
            cursor.executemany(sql_update, dados_para_atualizar)
            print(f"{len(dados_para_atualizar)} registros atualizados.")
        else:
            print("Nenhum registro para atualizar.")

        conn.commit()

    except Exception as e:
        print(f"Ocorreu um erro durante a carga de dados: {e}")
        if conn:
            conn.rollback() # Desfaz a transação em caso de erro
    finally:
        if conn:
            conn.close()

    # --- 3. VERIFICAÇÃO FINAL ---
    try:
        conn_verify = sqlite3.connect(db_path)
        df_verify = pd.read_sql(f'SELECT * FROM {table_name} ORDER BY SK_OCUP DESC LIMIT 5', conn_verify)
        print("\n--- Verificação dos últimos dados carregados no banco ---")
        print(df_verify)
        conn_verify.close()
    except Exception as e:
        print(f"Erro ao verificar os dados: {e}")

# --- Ponto de Entrada do Script ---
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Erro: Forneça o caminho para o arquivo CSV como um argumento.")
        print(f"Uso: python {os.path.basename(__file__)} <caminho_do_arquivo.csv>")
        sys.exit(1)

    caminho_do_csv = sys.argv[1]
    banco_de_dados = 'DW.db'
    
    if not os.path.exists(caminho_do_csv):
        print(f"Erro: O arquivo '{caminho_do_csv}' não existe ou o caminho está incorreto.")
        sys.exit(1)

    carregar_dados_merge(caminho_do_csv, banco_de_dados)
