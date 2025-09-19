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

df_tpocor = pd.DataFrame(data, columns=["CD_TPLOCOR", "DS_TPLOCOR"])

df_tpocor['SK_TPLOCOR'] = range(1, len(tp_ocorrencia_dict) + 1)
df_tpocor["DT_CARGA"] = datetime.now(br_tz).strftime("%d-%m-%Y %H:%M")
df_tpocor = df_tpocor[['SK_TPLOCOR', 'CD_TPLOCOR', 'DS_TPLOCOR', 'DT_CARGA']]

con = sqlite3.connect("DW.db")
cur = con.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS "DWCD_TPLOCOR" (
        "SK_TPLOCOR" INTEGER PRIMARY KEY AUTOINCREMENT,
        "CD_TPLOCOR" INTEGER NOT NULL UNIQUE,
        "DS_TPLOCOR" VARCHAR NOT NULL,
        "DT_CARGA" DATETIME NOT NULL
    );
""")

for _, row in df_tpocor.iterrows():
    cur.execute("""
        INSERT INTO DWCD_TPLOCOR (SK_TPLOCOR, CD_TPLOCOR, DS_TPLOCOR, DT_CARGA)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(SK_TPLOCOR) DO UPDATE SET
            CD_TPLOCOR = excluded.CD_TPLOCOR,
            DS_TPLOCOR = excluded.DS_TPLOCOR,
            DT_CARGA = excluded.DT_CARGA;
    """, tuple(row))


cur.execute("INSERT INTO DWCD_TPLOCOR (SK_TPLOCOR, CD_TPLOCOR, DS_TPLOCOR, DT_CARGA) VALUES (-1, -1, 'Não Informado', '28-11-1970 00:00')")

con.commit()
con.close()

try:
        conn_verify = sqlite3.connect("DW.db")
        df_verify = pd.read_sql(f'SELECT * FROM DWCD_TPLOCOR', conn_verify)
        print(f"\n--- Dados atuais na tabela DWCD_TPLOCOR ---")
        print(df_verify)
        conn_verify.close()
except Exception as e:
        print(f"Erro ao verificar os dados: {e}")