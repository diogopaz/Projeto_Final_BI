import pandas as pd
from datetime import datetime, timezone, timedelta
import sqlite3

br_tz = timezone(timedelta(hours=-3))
tp_ocorrencia_dict = {
    "1": "Via Pública",
    "2": "Endereço Residência",
    "3": "Outro Domicílio",
    "4": "Estabelecimento Comercial",
    "5": "Outros",
    "9": "Ignorado"
}

data = [(int(k), v) for k, v in tp_ocorrencia_dict.items()]

df_tpocor = pd.DataFrame(data, columns=["CD_TIPOBTOCOR", "DS_TIPO"])

df_tpocor['SK_TIPOBTOCOR'] = range(1, len(tp_ocorrencia_dict) + 1)
df_tpocor["DT_CARGA"] = datetime.now(br_tz).strftime("%d-%m-%Y %H:%M")
df_tpocor = df_tpocor[['SK_TIPOBTOCOR', 'CD_TIPOBTOCOR', 'DS_TIPO', 'DT_CARGA']]

df_tpocor

con = sqlite3.connect("DW.db")
cur = con.cursor()

for _, row in df_tpocor.iterrows():
    cur.execute("""
        INSERT INTO DWCD_TIPOBTOCOR (SK_TIPOBTOCOR, CD_TIPOBTOCOR, DS_TIPO, DT_CARGA)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(SK_TIPOBTOCOR) DO UPDATE SET
            CD_TIPOBTOCOR = excluded.CD_TIPOBTOCOR,
            DS_TIPO = excluded.DS_TIPO,
            DT_CARGA = excluded.DT_CARGA;
    """, tuple(row))

con.commit()
con.close()