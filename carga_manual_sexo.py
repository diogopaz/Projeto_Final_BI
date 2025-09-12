import pandas as pd
from datetime import datetime, timezone, timedelta
import sqlite3

br_tz = timezone(timedelta(hours=-3))
sexo_dict = {
    "0": "Ignorado",
    "1": "Masculino",
    "2": "Feminino",
    "9": "Ignorado",
    "M": "Masculino",
    "F": "Feminino",
    "I": "Ignorado"
}

data = [(k, v) for k, v in sexo_dict.items()]

df_sexo = pd.DataFrame(data, columns=["CD_SEXO", "DS_SEXO"])

df_sexo['SK_SEXO'] = range(1, len(sexo_dict) + 1)
df_sexo["DT_CARGA"] = datetime.now(br_tz).strftime("%d-%m-%Y %H:%M")
df_sexo = df_sexo[['SK_SEXO', 'CD_SEXO', 'DS_SEXO', 'DT_CARGA']]

con = sqlite3.connect("DW.db")
cur = con.cursor()

for _, row in df_sexo.iterrows():
    cur.execute("""
        INSERT INTO DWCD_SEXO (SK_SEXO, CD_SEXO, DS_SEXO, DT_CARGA)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(SK_SEXO) DO UPDATE SET
            CD_SEXO = excluded.CD_SEXO,
            DS_SEXO = excluded.DS_SEXO,
            DT_CARGA = excluded.DT_CARGA;
    """, tuple(row))

con.commit()
con.close()