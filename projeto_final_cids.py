import pandas as pd
from datetime import datetime, timezone, timedelta
import sqlite3
import sys

# Pega o caminho do JSON passado no terminal
if len(sys.argv) < 2:
    print("Uso: python script.py caminho/do/arquivo.json")
    sys.exit(1)

path = sys.argv[1]

br_tz = timezone(timedelta(hours=-3))

# Lê o JSON
cids = pd.read_json(path)
cids = pd.json_normalize(cids['data'])

# Cria SK e renomeia colunas
cids['SK_CID'] = range(1, len(cids) + 1)
cids.rename(columns={
    'codigo': 'CD_CID',
    'nome': 'NM_CID'
}, inplace=True)

# Cria campos derivados
cids['CD_CID_LINHA'] = ('*' + cids['CD_CID'].str.replace('.', '', regex=False))
cids.loc[cids['CD_CID_LINHA'].str.len() == 4, 'CD_CID_LINHA'] += 'X'
cids['CD_CID_CAUSA'] = (cids['CD_CID'].str.replace('.', '', regex=False))

# Reorganiza colunas
cids = cids[['SK_CID', 'CD_CID', 'CD_CID_LINHA', 'CD_CID_CAUSA', 'NM_CID']]
cids['DT_CARGA'] = datetime.now(br_tz).strftime('%d-%m-%Y %H:%M')

# Salva no banco SQLite
con = sqlite3.connect('DW.db')
cids.to_sql('DWCD_CIDS', con, if_exists="replace", index=False)
con.close()