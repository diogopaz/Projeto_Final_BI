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

def carregar_dime_causa():
    br_tz = timezone(timedelta(hours=-3))
    dt_carga = datetime.now(br_tz).strftime("%d-%m-%Y %H:%M")
    print("Carregando DIME_CAUSA...")
    sql_causa = """
        SELECT
            co.SK_CAUSAOBITO,
            COALESCE(cid_bas.CD_CID, 'Não Informado')   AS CD_CID_BASICA,
            COALESCE(cid_bas.DS_CID, 'Não Informado')   AS DS_CID_BASICA,
            COALESCE(cid_term.CD_CID, 'Não Informado')  AS CD_CID_TERMINAL,
            COALESCE(cid_term.DS_CID, 'Não Informado')  AS DS_CID_TERMINAL
        FROM DWCD_CAUSAOBITO co
        LEFT JOIN DWCD_CIDS cid_bas ON co.SK_CIDBAS = cid_bas.SK_CID
        LEFT JOIN DWCD_CIDS cid_term ON co.SK_CIDTERM = cid_term.SK_CID
    """
    dados = cursor_dw.execute(sql_causa).fetchall()
    dados_com_carga = [row + (dt_carga,) for row in dados]
    cursor_dm.executemany("""
        INSERT INTO DIME_CAUSA (
            SK_CAUSA, CD_CID_BASICA, DS_CID_BASICA, CD_CID_TERMINAL, DS_CID_TERMINAL, DT_CARGA
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, dados_com_carga)
    conn_dm.commit()
    print("-> Carga de DIME_CAUSA concluída.")

if __name__ == '__main__':
    try:
        carregar_dime_causa()
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        conn_dm.rollback()
    finally:
        conn_dw.close()
        conn_dm.close()