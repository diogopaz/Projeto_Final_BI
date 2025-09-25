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

def carregar_dime_pessoa():
    br_tz = timezone(timedelta(hours=-3))
    dt_carga = datetime.now(br_tz).strftime("%d-%m-%Y %H:%M")
    print("Carregando DIME_PESSOA...")
    sql_pessoa = """
        SELECT
            p.SK_PESSOA,
            COALESCE(s.CD_SEXO, -1), COALESCE(s.DS_SEXO, 'Não Informado'),
            COALESCE(r.CD_RACACOR, -1), COALESCE(r.DS_RACACOR, 'Não Informado'),
            COALESCE(c.CD_ESTCIV, -1), COALESCE(c.DS_ESTCIV, 'Não Informado'),
            COALESCE(o.CD_OCUP, -1), COALESCE(o.DS_OCUP, 'Não Informado'),
            COALESCE(e.CD_ESCFAL, -1), COALESCE(e.DS_ESCFAL, 'Não Informado'),
            p.DT_NASC
        FROM DWCD_PESSOA p
        LEFT JOIN DWCD_SEXO s ON p.SK_SEXO = s.SK_SEXO
        LEFT JOIN DWCD_RACACOR r ON p.SK_RACACOR = r.SK_RACACOR
        LEFT JOIN DWCD_ESTCIV c ON p.SK_ESTCIV = c.SK_ESTCIV
        LEFT JOIN DWCD_OCUP o ON p.SK_OCUP = o.SK_OCUP
        LEFT JOIN DWCD_ESCFAL e ON p.SK_ESCFAL = e.SK_ESCFAL
    """
    dados = cursor_dw.execute(sql_pessoa).fetchall()
    dados_com_carga = [row + (dt_carga,) for row in dados]
    cursor_dm.executemany("""
        INSERT INTO DIME_PESSOA (
            SK_PESSOA, CD_SEXO, DS_SEXO, CD_RACACOR, DS_RACACOR, CD_ESTCIV, DS_ESTCIV,
            CD_OCUP, DS_OCUP, CD_ESCFAL, DS_ESCFAL, DT_NASC, DT_CARGA
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, dados_com_carga)
    conn_dm.commit()
    print("-> Carga de DIME_PESSOA concluída.")

if __name__ == '__main__':
    try:
        carregar_dime_pessoa()
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        conn_dm.rollback()
    finally:
        conn_dw.close()
        conn_dm.close()