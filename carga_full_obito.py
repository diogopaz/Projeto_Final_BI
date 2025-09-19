import sys
import pandas as pd
from datetime import datetime, timezone, timedelta
import sqlite3
br_tz = timezone(timedelta(hours=-3))

df = pd.read_csv(sys.argv[1], sep=';')

df = df[df['CIRCOBITO'].notna()]
 
df = df[[      'TIPOBITO',
                         'CAUSABAS',
                         'DTOBITO',
                         'CODMUNOCOR',
                         'DTNASC',
                         'SEXO',
                         'RACACOR',
                         'ESCFALAGR1',
                         'ESTCIV',
                         'OCUP',
                         'ACIDTRAB',
                         'CIRCOBITO',
                         'TPOBITOCOR',
                         'CODMUNNATU',
                         'LINHAA',
                         'LINHAB',
                         'LINHAC',
                         'LINHAD',
                         'CODMUNRES']]
 
df = df.fillna(-1)
 
df["DTNASC"] = df["DTNASC"].astype(int)
df["DTNASC"] = df["DTNASC"].astype(str)
 
df['DTNASC'] = df['DTNASC'].apply(lambda x: x.zfill(8) if x != '-1' else x)
 
# Garante que seja string
df["DTOBITO"] = df["DTOBITO"].astype(int)
df["DTOBITO"] = df["DTOBITO"].astype(str)
df['DTOBITO'] = df['DTOBITO'].apply(lambda x: x.zfill(8) if x != '-1' else x)

def encontrar_causa_terminal(row):
    """varre as colunas de causa para encontrar a causa terminal (a primeira preenchida)"""
    for col in ['LINHAA', 'LINHAB', 'LINHAC', 'LINHAD']:
        # verifica se o valor não é nulo ou o valor padrão -1
        if row[col] != -1 and pd.notna(row[col]):
            return row[col]
    return -1 # retorna -1 se todas as linhas estiverem vazias ou com -1
 
def carga_final(df, db_path):
  conn = sqlite3.connect('DW.db')
  cursor = conn.cursor()
  dims = {
            'SEXO': pd.read_sql('SELECT SK_SEXO, CD_SEXO FROM DWCD_SEXO', conn),
            'RACACOR': pd.read_sql('SELECT SK_RACACOR, CD_RACACOR FROM DWCD_RACACOR', conn),
            'ESTCIV': pd.read_sql('SELECT SK_ESTCIV, CD_ESTCIV FROM DWCD_ESTCIV', conn),
            'ESCFAL': pd.read_sql('SELECT SK_ESCFAL, CD_ESCFAL FROM DWCD_ESCFAL', conn),
            'OCUP': pd.read_sql('SELECT SK_OCUP, CD_OCUP FROM DWCD_OCUP', conn),
            'MUNICIPIO': pd.read_sql('SELECT SK_MUNICIPIO, CD_MUNICIPIO FROM DWCD_MUNICIPIO', conn),
            'CIRCOBITO': pd.read_sql('SELECT SK_CIRCOBITO, CD_CIRCOBITO FROM DWCD_CIRCOBITO', conn),
            'TPLOCOR': pd.read_sql('SELECT SK_TPLOCOR, CD_TPLOCOR FROM DWCD_TPLOCOR', conn),
            'CIDS': pd.read_sql('SELECT * FROM DWCD_CIDS', conn)
            }
 
  cursor.execute("SELECT MAX(SK_PESSOA) FROM DWCD_PESSOA")
  ultimo_sk = cursor.fetchone()[0]
  if ultimo_sk is None:
    ultimo_sk = 1
  else:
    ultimo_sk += 1
 
  df_raw = df.copy()
 
  df_raw['CD_PESSOA'] = df_raw.apply(
      lambda row: f'{(int(row["RACACOR"]))}{int(row["SEXO"])}{int(row["DTNASC"])}{int(row["ESCFALAGR1"])}{int(row["ESTCIV"])}{int(row["OCUP"])}{int(row["CODMUNNATU"])}',
      axis=1
  )
  # pessoa
  pessoa_cols = ['CD_PESSOA','DTNASC', 'SEXO', 'RACACOR', 'ESCFALAGR1', 'ESTCIV', 'CODMUNNATU', 'OCUP']
  df_pessoa = df_raw[pessoa_cols].copy()
  df_pessoa = pd.merge(df_pessoa, dims['SEXO'], left_on='SEXO', right_on='CD_SEXO', how='left')
  df_pessoa = pd.merge(df_pessoa, dims['RACACOR'], left_on='RACACOR', right_on='CD_RACACOR', how='left')
  df_pessoa = pd.merge(df_pessoa, dims['ESCFAL'], left_on='ESCFALAGR1', right_on='CD_ESCFAL', how='left')
  df_pessoa = pd.merge(df_pessoa, dims['ESTCIV'], left_on='ESTCIV', right_on='CD_ESTCIV', how='left')
  df_pessoa = pd.merge(df_pessoa, dims['OCUP'], left_on='OCUP', right_on='CD_OCUP', how='left')
  df_pessoa = pd.merge(df_pessoa, dims['MUNICIPIO'], left_on='CODMUNNATU', right_on='CD_MUNICIPIO', how='left')
  df_pessoa = df_pessoa.rename(columns={'DTNASC': 'DT_NASC'})
  df_pessoa.fillna(-1, inplace=True)
 
  df_pessoas = df_pessoa.copy()
  df_pessoa[['CD_PESSOA', 'RACACOR', 'SEXO', 'DT_NASC', 'ESCFALAGR1', 'ESTCIV', 'OCUP', 'CODMUNNATU']]
  df_pessoa = df_pessoa.drop_duplicates(subset=['CD_PESSOA'])
 
  df_pessoa['DT_NASC'] = pd.to_datetime(df_pessoa['DT_NASC'], format='%d%m%Y', errors='coerce').dt.strftime('%d-%m-%Y')
  df_pessoa['DT_NASC'] = df_pessoa['DT_NASC'].fillna('-1')
 
  df_pessoas_existentes = pd.read_sql('SELECT CD_PESSOA FROM DWCD_PESSOA', conn)
 
  df_pessoa = df_pessoa[~df_pessoa['CD_PESSOA'].isin(df_pessoas_existentes['CD_PESSOA'])]
 
  existem_pessoas = (~df_pessoa['CD_PESSOA'].isin(df_pessoas_existentes['CD_PESSOA'])).any()
  df_pessoa = df_pessoa.drop_duplicates(subset='CD_PESSOA')
 
  if existem_pessoas:
    df_pessoa['SK_PESSOA'] = range(ultimo_sk, ultimo_sk + len(df_pessoa))
  else:
    df_pessoa['SK_PESSOA'] = -1
 
  df_pessoa = df_pessoa[['SK_PESSOA', 'CD_PESSOA', 'SK_SEXO', 'SK_RACACOR', 'SK_ESCFAL', 'SK_ESTCIV', 'SK_MUNICIPIO', 'SK_OCUP', 'DT_NASC']]
  # causa básica e terminal
  df_raw['CAUSATERMINAL'] = df_raw.apply(encontrar_causa_terminal, axis=1)
  df_raw['CD_CAUSAOBITO'] = df_raw.apply(
      lambda row: f'{(row["CAUSABAS"])}{row["CAUSATERMINAL"]}',
      axis=1
  )
 
  cursor.execute("SELECT MAX(SK_CAUSAOBITO) FROM DWCD_CAUSAOBITO")
  ultimo_sk = cursor.fetchone()[0]
  if ultimo_sk is None:
    ultimo_sk = 1
  else:
    ultimo_sk += 1
 
  causa_obito_cols = ['CD_CAUSAOBITO','CAUSABAS', 'CAUSATERMINAL']
  df_causaobito = df_raw[causa_obito_cols].copy()
  df_causaobito = pd.merge(df_causaobito, dims['CIDS'], left_on='CAUSATERMINAL', right_on='CD_CID_LINHA', how='left')
  df_causaobito = pd.merge(df_causaobito, dims['CIDS'], left_on='CAUSABAS', right_on='CD_CID_CAUSA', how='left')
  df_causaobito.fillna(-1, inplace=True)
 
  df_causaobito_existentes = pd.read_sql('SELECT CD_CAUSAOBITO FROM DWCD_CAUSAOBITO', conn)
  df_causaobito = df_causaobito[~df_causaobito['CD_CAUSAOBITO'].isin(df_causaobito_existentes['CD_CAUSAOBITO'])]
 
  existem_causas = (~df_causaobito['CD_CAUSAOBITO'].isin(df_causaobito_existentes['CD_CAUSAOBITO'])).any()
  df_causaobito = df_causaobito.drop_duplicates(subset='CD_CAUSAOBITO')
 
  if existem_causas:
    df_causaobito['SK_CAUSAOBITO'] = range(ultimo_sk, ultimo_sk + len(df_causaobito))
  else:
    df_causaobito['SK_CAUSAOBITO'] = -1
 
 
  df_causaobito = df_causaobito[['SK_CAUSAOBITO','CD_CAUSAOBITO','SK_CID_y', 'SK_CID_x']].rename(columns={'SK_CID_y': 'SK_CIDTERM', 'SK_CID_x': 'SK_CIDBAS'})
 
  # óbito
  obito_cols = ['CD_PESSOA', 'CD_CAUSAOBITO', 'TIPOBITO', 'DTOBITO', 'CODMUNOCOR', 'ACIDTRAB', 'CIRCOBITO', 'TPOBITOCOR']
  df_obito = df_raw[obito_cols].copy()
  df_obito = pd.merge(df_obito, df_causaobito, left_on='CD_CAUSAOBITO', right_on='CD_CAUSAOBITO', how='left')
  df_obito = pd.merge(df_obito, dims['CIRCOBITO'], left_on='CIRCOBITO', right_on='CD_CIRCOBITO', how='left')
  df_obito = pd.merge(df_obito, dims['TPLOCOR'], left_on='TPOBITOCOR', right_on='CD_TPLOCOR', how='left')
  df_obito = pd.merge(df_obito, dims['MUNICIPIO'], left_on='CODMUNOCOR', right_on='CD_MUNICIPIO', how='left')
  df_obito = pd.merge(df_obito, df_pessoa, left_on='CD_PESSOA', right_on='CD_PESSOA', how='left')
  df_obito.fillna(-1, inplace=True)
  df_obito = df_obito.rename(columns={'TIPOBITO': 'ST_FETAL', 'ACIDTRAB': 'ST_ACIDTRAB', 'DTOBITO': 'DT_OBITO', 'SK_MUNICIPIO_x': 'SK_MUNICIPIO'})
  df_obito = df_obito[['SK_MUNICIPIO', 'SK_PESSOA','SK_CAUSAOBITO', 'SK_CIRCOBITO', 'SK_TPLOCOR', 'ST_ACIDTRAB', 'ST_FETAL', 'DT_OBITO']]
  df_obito["DT_OBITO"] = pd.to_datetime(df_obito["DT_OBITO"].astype(str), format="%d%m%Y")
  df_obito["DT_OBITO"] = df_obito["DT_OBITO"].dt.strftime("%d-%m-%Y")
 
 
  dt_carga = datetime.now(br_tz).strftime('%d-%m-%Y %H:%M')
  df_pessoa['DT_CARGA'] = dt_carga
  df_causaobito['DT_CARGA'] = dt_carga
  df_obito['DT_CARGA'] = dt_carga
 
 
  df_pessoa.to_sql('DWCD_PESSOA', conn, if_exists='append', index=False, chunksize=10000)
  df_causaobito.to_sql('DWCD_CAUSAOBITO', conn, if_exists='append', index=False, chunksize=10000)
  df_obito.to_sql('DWMV_OBITO', conn, if_exists='append', index=False, chunksize=10000)
  conn.commit()
  conn.close()
 
carga_final(df, 'DW.db')