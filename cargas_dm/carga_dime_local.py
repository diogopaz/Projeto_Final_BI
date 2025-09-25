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

def carregar_dime_local():
    br_tz = timezone(timedelta(hours=-3))
    dt_carga = datetime.now(br_tz).strftime("%d-%m-%Y %H:%M")
    print("Carregando DIME_LOCAL...")
    sql = """
        SELECT
            m.SK_MUNICIPIO, m.CD_MUNICIPIO, m.NM_MUNICIPIO, m.ST_CAPITAL,
            e.CD_ESTADO, e.NM_ESTADO, e.SK_REGIAO, r.NM_REGIAO
        FROM DWCD_MUNICIPIO m
        JOIN DWCD_ESTADO e ON m.SK_ESTADO = e.SK_ESTADO
        JOIN DWCD_REGIAO r ON e.SK_REGIAO = r.SK_REGIAO
    """
    dados = cursor_dw.execute(sql).fetchall()
    dados_com_carga = [row + (dt_carga,) for row in dados]
    cursor_dm.executemany("""
        INSERT INTO DIME_LOCAL (
            SK_LOCAL, CD_MUNICIPIO, NM_MUNICIPIO, ST_CAPITAL,
            CD_ESTADO, NM_ESTADO, CD_REGIAO, NM_REGIAO, DT_CARGA
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, dados_com_carga)
    conn_dm.commit()
    print("-> Carga de DIME_LOCAL concluída.")

if __name__ == '__main__':
    try:
        carregar_dime_local()
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        conn_dm.rollback()
    finally:
        conn_dw.close()
        conn_dm.close()