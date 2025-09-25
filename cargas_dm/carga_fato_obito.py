import sqlite3
from datetime import datetime, timedelta, timezone
import pandas as pd

# --- config ---
dw_path = 'DW.db'
dm_path = 'DM.db'

# --- conexoes ---
conn_dw = sqlite3.connect(dw_path)
cursor_dw = conn_dw.cursor()
conn_dm = sqlite3.connect(dm_path)
cursor_dm = conn_dm.cursor()

def carregar_fato_obito():
    br_tz = timezone(timedelta(hours=-3))
    dt_carga = datetime.now(br_tz).strftime("%d-%m-%Y %H:%M")
    print("Carregando FATO_OBITO...")
    
    print("  -> Criando mapa de datas para lookup...")
    cursor_dm.execute("SELECT DATA, SK_TEMPO_DIA FROM DIME_TEMPO_DIA")
    data_sk_map = {row[0]: row[1] for row in cursor_dm.fetchall()}
    
    sql_obito = """
        SELECT
            o.CD_OBITO, o.SK_PESSOA, o.SK_MUNICIPIO, p.SK_MUNICIPIO, o.DT_OBITO,
            o.SK_CAUSAOBITO, o.SK_CIRCOBITO, o.SK_TPLOCOR, o.ST_FETAL, o.ST_ACIDTRAB
        FROM DWMV_OBITO o
        JOIN DWCD_PESSOA p ON o.SK_PESSOA = p.SK_PESSOA
    """
    dados_obito = cursor_dw.execute(sql_obito).fetchall()

    dados_para_inserir_obito = []
    for row in dados_obito:
        cd_obito, sk_pessoa, sk_local_obito, sk_local_nasc, dt_obito_str, sk_causa, sk_circobito, sk_tplocor, st_fetal, st_acidtrab = row
        sk_dt_obito = data_sk_map.get(dt_obito_str, -1)
        dados_para_inserir_obito.append((
            cd_obito, sk_pessoa, sk_local_obito, sk_local_nasc, sk_dt_obito, sk_causa,
            sk_circobito, sk_tplocor, st_fetal, st_acidtrab, 1, dt_carga
        ))

    cursor_dm.executemany("""
        INSERT INTO FATO_OBITO (
            CD_OBITO, SK_PESSOA, SK_LOCAL_OBITO, SK_LOCAL_NASC, SK_DT_OBITO, SK_CAUSA,
            SK_CIRCOBITO, SK_TPLOCOR, ST_FETAL, ST_ACIDTRAB, QTD_OBITO, DT_CARGA
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, dados_para_inserir_obito)
    conn_dm.commit()
    print("-> Carga de FATO_OBITO concluída.")

if __name__ == '__main__':
    try:
        carregar_fato_obito()
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        conn_dm.rollback()
    finally:
        conn_dw.close()
        conn_dm.close()