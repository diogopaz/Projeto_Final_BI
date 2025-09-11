import pandas as pd
import sqlite3
from datetime import datetime, timezone, timedelta

def carga_manual_estciv(db_path):
    
    dados_estciv = [
        (-1, 'Não Informado'),
        (1, 'Solteiro'),
        (2, 'Casado'),
        (3, 'Viúvo'),
        (4, 'Separado Judicialmente'),
        (5, 'União Estável')
    ]
    
    table_name = "DWCD_ESTCIV"
    br_tz = timezone(timedelta(hours=-3))
    data_carga_atual = datetime.now(br_tz).strftime('%d-%m-%Y %H:%M')
    conn = None

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # garantir que a tabela existe
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                "SK_ESTCIV" INTEGER PRIMARY KEY AUTOINCREMENT,
                "CD_ESTCIV" INTEGER NOT NULL UNIQUE,
                "DS_ESTCIV" VARCHAR NOT NULL,
                "DT_CARGA" DATETIME NOT NULL
            );
        """)
        conn.commit()


        # data da carga
        dados_para_inserir = [
            (cd, ds, data_carga_atual) for cd, ds in dados_estciv
        ]

        # inserindo valores na tabela
        sql_insert = f'INSERT OR IGNORE INTO "{table_name}" (CD_ESTCIV, DS_ESTCIV, DT_CARGA) VALUES (?, ?, ?)'
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

    carga_manual_estciv(banco_de_dados)