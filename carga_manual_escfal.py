import pandas as pd
from datetime import datetime, timezone, timedelta
import sqlite3

br_tz = timezone(timedelta(hours=-3))
escolaridade_dict = {
    "00": "Sem Escolaridade",
    "01": "Fundamental I Incompleto",
    "02": "Fundamental I Completo",
    "03": "Fundamental II Incompleto",
    "04": "Fundamental II Completo",
    "05": "Ensino Médio Incompleto",
    "06": "Ensino Médio Completo",
    "07": "Superior Incompleto",
    "08": "Superior Completo",
    "09": "Ignorado",
    "10": "Fundamental I Incompleto ou Inespecífico",
    "11": "Fundamental II Incompleto ou Inespecífico",
    "12": "Ensino Médio Incompleto ou Inespecífico"
}

data = [(int(k), v) for k, v in escolaridade_dict.items()]

df_escolaridade = pd.DataFrame(data, columns=["CD_ESCFAL", "DS_ESCFAL"])

df_escolaridade['SK_ESCFAL'] = range(1, len(escolaridade_dict) + 1)
df_escolaridade["DT_CARGA"] = datetime.now(br_tz).strftime("%d-%m-%Y %H:%M")
df_escolaridade = df_escolaridade[['SK_ESCFAL', 'CD_ESCFAL', 'DS_ESCFAL', 'DT_CARGA']]

con = sqlite3.connect("DW.db")
cur = con.cursor()

for _, row in df_escolaridade.iterrows():
    cur.execute("""
        INSERT INTO DWCD_ESCFAL (SK_ESCFAL, CD_ESCFAL, DS_ESCFAL, DT_CARGA)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(SK_ESCFAL) DO UPDATE SET
            CD_ESCFAL = excluded.CD_ESCFAL,
            DS_ESCFAL = excluded.DS_ESCFAL,
            DT_CARGA = excluded.DT_CARGA;
    """, tuple(row))

con.commit()
con.close()