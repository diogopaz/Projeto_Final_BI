import sys
import pandas as pd
from datetime import datetime, timezone, timedelta
import sqlite3
import hashlib

br_tz = timezone(timedelta(hours=-3))

df = pd.read_csv(sys.argv[1], sep=';')

df = df[df['CIRCOBITO'].notna()]

df = df[[ 'TIPOBITO', 'CAUSABAS', 'DTOBITO', 'CODMUNOCOR', 'DTNASC', 'SEXO',
           'RACACOR', 'ESCFALAGR1', 'ESTCIV', 'OCUP', 'ACIDTRAB', 'CIRCOBITO',
           'TPOBITOCOR', 'CODMUNNATU', 'LINHAA', 'LINHAB', 'LINHAC', 'LINHAD', 'CODMUNRES']]

df['ACIDTRAB'] = df['ACIDTRAB'].fillna(2).replace(9, 2)
df = df.fillna(-1)

df["DTNASC"] = df["DTNASC"].astype(int).astype(str).apply(lambda x: x.zfill(8) if x != '-1' else x)
df["DTOBITO"] = df["DTOBITO"].astype(int).astype(str).apply(lambda x: x.zfill(8) if x != '-1' else x)

def criar_cd_pessoa_hash(row):
    """
    Cria uma chave de negócio (CD_PESSOA) única e segura usando um hash MD5 truncado.
    """
    colunas_chave = [
        str(row['RACACOR']), str(row['SEXO']), str(row['DTNASC']),
        str(row['ESCFALAGR1']), str(row['ESTCIV']), str(row['OCUP']),
        str(row['CODMUNNATU'])
    ]
    string_unica = '|'.join(colunas_chave)
    hash_completo = hashlib.md5(string_unica.encode('utf-8')).hexdigest()
    return hash_completo[:16]

def encontrar_causa_terminal(row):
    for col in ['LINHAA', 'LINHAB', 'LINHAC', 'LINHAD']:
        if row[col] != -1 and pd.notna(row[col]):
            return f"*{row[col].split('*')[1]}"
    return -1

# --- FUNÇÃO DE CARGA ---
def carga_final(df_raw, db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    dt_carga = datetime.now(br_tz).strftime('%d-%m-%Y %H:%M')

    # 1. LER DIMENSÕES EXISTENTES DO DW
    dims = {
        'SEXO': pd.read_sql('SELECT SK_SEXO, CD_SEXO FROM DWCD_SEXO', conn),
        'RACACOR': pd.read_sql('SELECT SK_RACACOR, CD_RACACOR FROM DWCD_RACACOR', conn),
        'ESTCIV': pd.read_sql('SELECT SK_ESTCIV, CD_ESTCIV FROM DWCD_ESTCIV', conn),
        'ESCFAL': pd.read_sql('SELECT SK_ESCFAL, CD_ESCFAL FROM DWCD_ESCFAL', conn),
        'OCUP': pd.read_sql('SELECT SK_OCUP, CD_OCUP FROM DWCD_OCUP', conn),
        'MUNICIPIO': pd.read_sql('SELECT SK_MUNICIPIO, CD_MUNICIPIO FROM DWCD_MUNICIPIO', conn),
        'CIRCOBITO': pd.read_sql('SELECT SK_CIRCOBITO, CD_CIRCOBITO FROM DWCD_CIRCOBITO', conn),
        'TPLOCOR': pd.read_sql('SELECT SK_TPLOCOR, CD_TPLOCOR FROM DWCD_TPLOCOR', conn),
        'CIDS': pd.read_sql('SELECT SK_CID, CD_CID_LINHA, CD_CID_CAUSA FROM DWCD_CIDS', conn)
    }

    # --- 2. PROCESSAR E CARREGAR DIMENSÃO PESSOA (INCREMENTAL) ---
    print("Processando dimensão Pessoa...")
    
    df_raw['CD_PESSOA'] = df_raw.apply(criar_cd_pessoa_hash, axis=1)
    df_pessoa_origem = df_raw[['CD_PESSOA', 'DTNASC', 'SEXO', 'RACACOR', 'ESCFALAGR1', 'ESTCIV', 'CODMUNNATU', 'OCUP']].drop_duplicates()
    
    pessoas_existentes = pd.read_sql('SELECT CD_PESSOA FROM DWCD_PESSOA', conn)
    df_pessoa_novas = df_pessoa_origem[~df_pessoa_origem['CD_PESSOA'].isin(pessoas_existentes['CD_PESSOA'])].copy()

    if not df_pessoa_novas.empty:
        cursor.execute("SELECT MAX(SK_PESSOA) FROM DWCD_PESSOA")
        ultimo_sk = (cursor.fetchone()[0] or 0) + 1
        df_pessoa_novas['SK_PESSOA'] = range(ultimo_sk, ultimo_sk + len(df_pessoa_novas))
        
        # Junções para obter SKs
        df_pessoa_novas = pd.merge(df_pessoa_novas, dims['SEXO'], left_on='SEXO', right_on='CD_SEXO', how='left')
        df_pessoa_novas = pd.merge(df_pessoa_novas, dims['RACACOR'], left_on='RACACOR', right_on='CD_RACACOR', how='left')
        df_pessoa_novas = pd.merge(df_pessoa_novas, dims['ESCFAL'], left_on='ESCFALAGR1', right_on='CD_ESCFAL', how='left')
        df_pessoa_novas = pd.merge(df_pessoa_novas, dims['ESTCIV'], left_on='ESTCIV', right_on='CD_ESTCIV', how='left')
        df_pessoa_novas = pd.merge(df_pessoa_novas, dims['OCUP'], left_on='OCUP', right_on='CD_OCUP', how='left')
        df_pessoa_novas = pd.merge(df_pessoa_novas, dims['MUNICIPIO'], left_on='CODMUNNATU', right_on='CD_MUNICIPIO', how='left')
        df_pessoa_novas.fillna(-1, inplace=True)
        
        df_pessoa_novas['DT_NASC'] = pd.to_datetime(df_pessoa_novas['DTNASC'], format='%d%m%Y', errors='coerce').dt.strftime('%d-%m-%Y').fillna('-1')
        df_pessoa_novas['DT_CARGA'] = dt_carga
        
        colunas_pessoa_dw = ['SK_PESSOA', 'CD_PESSOA', 'SK_SEXO', 'SK_RACACOR', 'SK_ESCFAL', 'SK_ESTCIV', 'SK_MUNICIPIO', 'SK_OCUP', 'DT_NASC', 'DT_CARGA']
        df_pessoa_novas[colunas_pessoa_dw].to_sql('DWCD_PESSOA', conn, if_exists='append', index=False)
        print(f"   -> {len(df_pessoa_novas)} novas pessoas inseridas.")

    # --- 3. PROCESSAR E CARREGAR DIMENSÃO CAUSAOBITO (INCREMENTAL) ---
    print("Processando dimensão Causa do Óbito...")

    causas_encontradas = df_raw.apply(encontrar_causa_terminal, axis=1)

    df_raw['CAUSATERMINAL'] = causas_encontradas.where(
    causas_encontradas != -1,
    df_raw['CAUSABAS'].apply(
        lambda x: f"*{x}X" if isinstance(x, str) and len(x) == 3 else f"*{x}"
    )
)

    df_raw['CD_CAUSAOBITO'] = df_raw.apply(lambda r: f"{r['CAUSABAS']}{r['CAUSATERMINAL']}", axis=1)
    
    df_causa_origem = df_raw[['CD_CAUSAOBITO', 'CAUSABAS', 'CAUSATERMINAL']].drop_duplicates()
    
    causas_existentes = pd.read_sql('SELECT CD_CAUSAOBITO FROM DWCD_CAUSAOBITO', conn)
    df_causa_novas = df_causa_origem[~df_causa_origem['CD_CAUSAOBITO'].isin(causas_existentes['CD_CAUSAOBITO'])].copy()

    if not df_causa_novas.empty:
        cursor.execute("SELECT MAX(SK_CAUSAOBITO) FROM DWCD_CAUSAOBITO")
        ultimo_sk = (cursor.fetchone()[0] or 0) + 1
        df_causa_novas['SK_CAUSAOBITO'] = range(ultimo_sk, ultimo_sk + len(df_causa_novas))
        
        df_causa_novas = pd.merge(df_causa_novas, dims['CIDS'], left_on='CAUSABAS', right_on='CD_CID_CAUSA', how='left')
        df_causa_novas = pd.merge(df_causa_novas, dims['CIDS'], left_on='CAUSATERMINAL', right_on='CD_CID_LINHA', how='left', suffixes=('_bas', '_term'))
        df_causa_novas.fillna(-1, inplace=True)
        df_causa_novas['DT_CARGA'] = dt_carga
        
        colunas_causa_dw = ['SK_CAUSAOBITO', 'CD_CAUSAOBITO', 'SK_CID_bas', 'SK_CID_term', 'DT_CARGA']
        df_causa_novas[colunas_causa_dw].rename(columns={'SK_CID_bas': 'SK_CIDBAS', 'SK_CID_term': 'SK_CIDTERM'}).to_sql('DWCD_CAUSAOBITO', conn, if_exists='append', index=False)
        print(f"   -> {len(df_causa_novas)} novas causas inseridas.")
        
    # --- 4. MONTAR E CARREGAR TABELA FATO DWMV_OBITO ---
    print("Montando e carregando a tabela Fato...")
    # Ler as dimensões completas para lookup
    pessoas_lookup = pd.read_sql('SELECT SK_PESSOA, CD_PESSOA FROM DWCD_PESSOA', conn)
    causas_lookup = pd.read_sql('SELECT SK_CAUSAOBITO, CD_CAUSAOBITO FROM DWCD_CAUSAOBITO', conn)
    
    df_obito = df_raw[['CD_PESSOA', 'CD_CAUSAOBITO', 'TIPOBITO', 'DTOBITO', 'CODMUNOCOR', 'ACIDTRAB', 'CIRCOBITO', 'TPOBITOCOR']]
    
    # Junções com os lookups completos
    df_obito = pd.merge(df_obito, pessoas_lookup, on='CD_PESSOA', how='left')
    df_obito = pd.merge(df_obito, causas_lookup, on='CD_CAUSAOBITO', how='left')
    df_obito = pd.merge(df_obito, dims['CIRCOBITO'], left_on='CIRCOBITO', right_on='CD_CIRCOBITO', how='left')
    df_obito = pd.merge(df_obito, dims['TPLOCOR'], left_on='TPOBITOCOR', right_on='CD_TPLOCOR', how='left')
    df_obito = pd.merge(df_obito, dims['MUNICIPIO'], left_on='CODMUNOCOR', right_on='CD_MUNICIPIO', how='left')
    df_obito.fillna(-1, inplace=True)

    df_obito['DT_OBITO'] = pd.to_datetime(df_obito['DTOBITO'], format='%d%m%Y', errors='coerce').dt.strftime('%d-%m-%Y').fillna('-1')
    df_obito['ST_FETAL'] = (df_obito['TIPOBITO'] == 1).astype(int)
    df_obito['ST_ACIDTRAB'] = (df_obito['ACIDTRAB'] == 1).astype(int)
    df_obito['DT_CARGA'] = dt_carga
    
    colunas_fato_dw = ['SK_MUNICIPIO', 'SK_CAUSAOBITO', 'SK_PESSOA', 'SK_CIRCOBITO', 'SK_TPLOCOR', 'ST_ACIDTRAB', 'ST_FETAL', 'DT_OBITO', 'DT_CARGA']
    df_obito[colunas_fato_dw].to_sql('DWMV_OBITO', conn, if_exists='append', index=False, chunksize=10000)
    print(f"   -> {len(df_obito)} registros de óbito inseridos.")
    
    conn.commit()
    conn.close()

carga_final(df, 'DW.db')