def carga_manual_racacor(db_path):

    dados_racacor = [
        (1, 'Branca'),
        (2, 'Preta'),
        (3, 'Amarela'),
        (4, 'Parda'),
        (5, 'Indígena')
    ]

    table_name = "DWCD_RACACOR"
    br_tz = timezone(timedelta(hours=-3))
    data_carga_atual = datetime.now(br_tz).strftime('%d-%m-%Y %H:%M')
    conn = None

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # garantir que a tabela existe
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                "SK_RACACOR" INTEGER PRIMARY KEY AUTOINCREMENT,
                "CD_RACACOR" INTEGER NOT NULL UNIQUE,
                "DS_RACACOR" VARCHAR NOT NULL,
                "DT_CARGA" DATETIME NOT NULL
            );
        """)
        conn.commit()

        # adicionando a data da carga
        dados_para_inserir = [
            (cd, ds, data_carga_atual) for cd, ds in dados_racacor
        ]

        # inserindo valores na tabela
        sql_insert = f'INSERT OR IGNORE INTO "{table_name}" (CD_RACACOR, DS_RACACOR, DT_CARGA) VALUES (?, ?, ?)'
        cursor.executemany(sql_insert, dados_para_inserir)
        cursor.execute(f"INSERT OR IGNORE INTO {table_name} (SK_RACACOR, CD_RACACOR, DS_RACACOR, DT_CARGA) VALUES (-1, -1, 'Não Informado', '28-11-1970 00:00')")
        conn.commit()

        print("Carga concluída.")

    except sqlite3.Error as e:
        print(f"Ocorreu um erro de banco de dados: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")
    finally:
        if conn:
            conn.close()

    # mostrando dados atuais da tabela racacor
    try:
        conn_verify = sqlite3.connect(db_path)
        df_verify = pd.read_sql(f'SELECT * FROM {table_name}', conn_verify)
        print(f"\n--- Dados atuais na tabela '{table_name}' ---")
        print(df_verify)
        conn_verify.close()
    except Exception as e:
        print(f"Erro ao verificar os dados: {e}")

if __name__ == "__main__":
    banco_de_dados = 'DW.db'

    carga_manual_racacor(banco_de_dados)