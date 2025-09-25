import sqlite3
from datetime import datetime, timedelta, timezone

# --- config ---
dw_path = 'DW.db'
dm_path = 'DM.db'

# --- conexoes ---
conn_dw = sqlite3.connect(dw_path)
cursor_dw = conn_dw.cursor()
conn_dm = sqlite3.connect(dm_path)
cursor_dm = conn_dm.cursor()

def carregar_dime_tempo_ano():
    br_tz = timezone(timedelta(hours=-3))
    dt_carga = datetime.now(br_tz).strftime("%d-%m-%Y %H:%M")
    print("Carregando DIME_TEMPO_ANO...")
    anos_unicos = cursor_dw.execute("SELECT DISTINCT ANO FROM DWMV_POPULACAO ORDER BY ANO").fetchall()
    dados_com_carga = []
    for sk, ano_tuple in enumerate(anos_unicos, start=1):
        ano = ano_tuple[0]
        dados_com_carga.append((sk, ano, dt_carga))
    cursor_dm.executemany("INSERT INTO DIME_TEMPO_ANO (SK_TEMPO_ANO, ANO, DT_CARGA) VALUES (?, ?, ?)", dados_com_carga)
    conn_dm.commit()
    print("-> Carga de DIME_TEMPO_ANO concluída.")

if __name__ == '__main__':
    try:
        carregar_dime_tempo_ano()
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        conn_dm.rollback()
    finally:
        conn_dw.close()
        conn_dm.close()