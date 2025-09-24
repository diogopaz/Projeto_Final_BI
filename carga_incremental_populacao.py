import pandas as pd
from datetime import datetime, timezone, timedelta
import sys
import sqlite3

filename = sys.argv[1]
db_path = 'DW.db'
populacao_mun = pd.read_excel(filename, sheet_name=1,  header=1)
parts = filename.split('_')
year_split = parts[-1].split('.')[0]
br_tz = timezone(timedelta(hours=-3))

def populacao(df, ano):
  populacao_mun = df.copy()
  populacao_mun = populacao_mun.dropna()
  populacao_mun['COD. UF'] = populacao_mun['COD. UF'].astype(int)
  populacao_mun['COD. UF'] = populacao_mun['COD. UF'].astype(str)
  populacao_mun['COD. MUNIC'] = populacao_mun['COD. MUNIC'].astype(int)
  populacao_mun['COD. MUNIC'] = populacao_mun['COD. MUNIC'].astype(str).str.zfill(5)
  populacao_mun['CD_MUNICIPIO'] = populacao_mun['COD. UF'] + populacao_mun['COD. MUNIC']
  populacao_mun['CD_MUNICIPIO'] = (
      populacao_mun['CD_MUNICIPIO']
      .astype(str)
      .str[:6]
      .astype(int)
  )
  populacao_mun = populacao_mun.rename(columns={'POPULAÇÃO ESTIMADA': 'QTD_POPULACAO'})
  populacao_mun['QTD_POPULACAO'] = populacao_mun['QTD_POPULACAO'].astype(str).str.replace(r'[^0-9]', '', regex=True)
  populacao_mun['QTD_POPULACAO'] = populacao_mun['QTD_POPULACAO'].astype(int)
  populacao_mun['ANO'] = ano
  populacao_mun["DT_CARGA"] = datetime.now(br_tz).strftime("%d-%m-%Y %H:%M")

  populacao_mun_tratado = populacao_mun[['CD_MUNICIPIO', 'ANO', 'QTD_POPULACAO', 'DT_CARGA']]
  return populacao_mun_tratado

mun = populacao(populacao_mun, year_split)
conn = sqlite3.connect(db_path)
ibge_municipio = pd.read_sql('SELECT SK_MUNICIPIO, CD_MUNICIPIO FROM DWCD_MUNICIPIO', conn)
mun = mun.merge(ibge_municipio[["SK_MUNICIPIO", "CD_MUNICIPIO"]],
    on="CD_MUNICIPIO",
    how="inner")

mun = mun[['SK_MUNICIPIO', 'ANO', 'QTD_POPULACAO', 'DT_CARGA']]
mun.to_sql(
    "DWMV_POPULACAO",
    conn,
    if_exists="append",
    index=False
)
conn.close()