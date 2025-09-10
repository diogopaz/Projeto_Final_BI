import pandas as pd
import sqlite3
from datetime import datetime, timezone, timedelta
import sys
import os

def carregar_dados(csv_path, db_path):
    """
    Executa o processo de ETL para carregar dados de ocupações de um CSV para o SQLite.
    :param csv_path: Caminho para o arquivo CSV de origem.
    :param db_path: Caminho para o arquivo de banco de dados SQLite.
    """
    # lendo csv
    try:
        df_ocupacao_raw = pd.read_csv(csv_path, encoding='latin-1', sep=';')
        print("--- DataFrame de Origem (Amostra) ---")
        print(df_ocupacao_raw.head())
    except FileNotFoundError:
        print(f"Erro: Arquivo não encontrado no caminho especificado: '{csv_path}'")
        return
    except Exception as e:
        print(f"Ocorreu um erro ao ler o arquivo CSV: {e}")
        return

    # verificando se as colunas existem no csv
    if 'CODIGO' not in df_ocupacao_raw.columns or 'TITULO' not in df_ocupacao_raw.columns:
        print("Erro: O arquivo CSV de entrada deve conter as colunas 'CODIGO' e 'TITULO'.")
        return

    # tratando nomes de colunas
    df_dim_ocupacao = df_ocupacao_raw.rename(columns={
        'CODIGO': 'CD_OCUPACAO',
        'TITULO': 'DS_TITULO'
    })
    
    # fuso horário de Brasília
    br_tz = timezone(timedelta(hours=-3))
    data_carga_atual = datetime.now(br_tz).strftime('%d-%m-%Y %H:%M')
    
    df_dim_ocupacao['DT_CARGA'] = data_carga_atual

    # carga
    table_name = "DWCD_OCUPACAO"
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        cursor.execute(
            f"""
            CREATE TABLE "{table_name}" (
                "SK_OCUPACAO" INTEGER NOT NULL UNIQUE,
                "CD_OCUPACAO" INTEGER NOT NULL UNIQUE,
                "DS_TITULO" VARCHAR NOT NULL,
                "DT_CARGA" DATETIME,
                PRIMARY KEY("SK_OCUPACAO" AUTOINCREMENT)
            );
            """
        )
        dados_para_inserir = df_dim_ocupacao.to_records(index=False).tolist()
        sql_insert = f'INSERT INTO "{table_name}" (CD_OCUPACAO, DS_TITULO, DT_CARGA) VALUES (?, ?, ?)'
        cursor.executemany(sql_insert, dados_para_inserir)
        conn.commit()
        print(f"\nCarga de dados para a tabela '{table_name}' concluída com sucesso!")

    except Exception as e:
        print(f"Ocorreu um erro durante a carga de dados: {e}")

    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # verifica se foi passado o caminho do arquivo fonte
    if len(sys.argv) != 2:
        print("Erro: Forneça o caminho para o arquivo CSV como um argumento.")
        print("Uso: python etl_dw_ocupacao.py <caminho_do_arquivo.csv>")
        sys.exit(1)

    caminho_do_csv = sys.argv[1]
    banco_de_dados = 'DW.db'
    
    # verifica se o arquivo passado existe
    if not os.path.exists(caminho_do_csv):
        print(f"Erro: O arquivo '{caminho_do_csv}' não existe ou o caminho está incorreto.")
        sys.exit(1)

    carregar_dados(caminho_do_csv, banco_de_dados)

