import sqlite3
from datetime import datetime, timezone, timedelta
import pandas as pd

def carga_manual_circobito(db_path):

    dados_circobito = [
        (-1, 'Não Informado'),
        (1, 'Acidente',),
        (2, 'Suicídio'),
        (3, 'Homicídio'),
        (4, 'Outros')
    ]

    table_name = "DWCD_CIRCOBITO"
    br_tz = timezone(timedelta(hours=-3))
    data_carga_atual = datetime.now(br_tz).strftime('%d-%m-%Y %H:%M')
    conn = None

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                "SK_CIRCOBITO" INTEGER PRIMARY KEY AUTOINCREMENT,
                "CD_CIRCOBITO" INTEGER NOT NULL UNIQUE,
                "DS_CIRCOBITO" VARCHAR NOT NULL,
                "DT_CARGA" DATETIME NOT NULL
            );
        """)
        conn.commit()


        # data da carga
        dados_para_inserir = [
            (cd, ds, data_carga_atual) for cd, ds in dados_circobito
        ]

        # inserindo valores na tabela
        sql_insert = f'INSERT OR IGNORE INTO "{table_name}" (CD_CIRCOBITO, DS_CIRCOBITO, DT_CARGA) VALUES (?, ?, ?)'
        cursor.executemany(sql_insert, dados_para_inserir)
        conn.commit()

    except sqlite3.Error as e:
        print(f"Ocorreu um erro de banco de dados: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")
    finally:
        if conn:
            conn.close()

    try:
        conn_verify = sqlite3.connect(db_path)
        df_verify = pd.read_sql(f'SELECT * FROM {table_name}', conn_verify)
        print(f"\n--- Dados atuais na tabela '{table_name}' ---")
        print(df_verify)
        conn_verify.close()
    except NameError:
        print("\nPandas não importado. A verificação final não pode ser executada.")
    except Exception as e:
        print(f"Erro ao verificar os dados: {e}")

if __name__ == "__main__":
    banco_de_dados = 'DW.db'
    carga_manual_circobito(banco_de_dados)
