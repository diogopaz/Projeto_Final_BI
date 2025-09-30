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

def carregar_dime_circobito():
    br_tz = timezone(timedelta(hours=-3))
    dt_carga = datetime.now(br_tz).strftime("%d-%m-%Y %H:%M")
    print("Carregando DIME_CIRCOBITO...")
    dados = cursor_dw.execute("SELECT SK_CIRCOBITO, CD_CIRCOBITO, DS_CIRCOBITO FROM DWCD_CIRCOBITO").fetchall()
    dados_com_carga = [row + (dt_carga,) for row in dados]
    cursor_dm.executemany("INSERT INTO DIME_CIRCOBITO (SK_CIRCOBITO, CD_CIRCOBITO, DS_CIRCOBITO, DT_CARGA) VALUES (?, ?, ?, ?)", dados_com_carga)
    conn_dm.commit()
    print("-> Carga de DIME_CIRCOBITO concluída.")

if __name__ == '__main__':
    try:
        carregar_dime_circobito()
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        conn_dm.rollback()
    finally:
        conn_dw.close()
        conn_dm.close()