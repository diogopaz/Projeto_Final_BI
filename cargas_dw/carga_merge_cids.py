import pandas as pd
from datetime import datetime, timezone, timedelta
import sqlite3
import sys

# Pega o caminho do JSON passado no terminal
if len(sys.argv) < 2:
    print("Uso: python script.py caminho/do/arquivo.json")
    sys.exit(1)

path = sys.argv[1]
db_path = 'DW.db'
table_name = 'DWCD_CIDS'
br_tz = timezone(timedelta(hours=-3))
data_carga_atual = datetime.now(br_tz).strftime('%d-%m-%Y %H:%M')

# --- 1. LEITURA E TRANSFORMAÇÃO DA ORIGEM ---
print("Lendo e tratando dados de origem (JSON)...")
try:
    df_origem = pd.read_json(path)
    df_origem = pd.json_normalize(df_origem['data'])
    
    # Renomeia colunas
    df_origem.rename(columns={
        'codigo': 'CD_CID',
        'nome': 'DS_CID'
    }, inplace=True)

    # Cria campos derivados (sua lógica original)
    df_origem['CD_CID_LINHA'] = ('*' + df_origem['CD_CID'].str.replace('.', '', regex=False))
    df_origem.loc[df_origem['CD_CID_LINHA'].str.len() == 4, 'CD_CID_LINHA'] += 'X'
    df_origem['CD_CID_CAUSA'] = (df_origem['CD_CID'].str.replace('.', '', regex=False))
    
    # Adiciona data de carga e seleciona colunas
    df_origem['DT_CARGA'] = data_carga_atual
    df_origem = df_origem[['CD_CID', 'CD_CID_LINHA', 'CD_CID_CAUSA', 'DS_CID', 'DT_CARGA']]
except Exception as e:
    print(f"Erro ao processar o arquivo de origem: {e}")
    sys.exit(1)


# --- LÓGICA DE MERGE ---
conn = None
try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # --- 2. LEITURA DOS DADOS DE DESTINO ---
    print("Lendo dados existentes no DW...")
    try:
        # Lê apenas as colunas necessárias para a comparação
        df_destino = pd.read_sql(f'SELECT CD_CID, DS_CID FROM {table_name}', conn)
    except pd.io.sql.DatabaseError:
        # Caso a tabela ainda não exista, cria um DataFrame vazio
        df_destino = pd.DataFrame(columns=['CD_CID', 'DS_CID'])

    # --- 3. COMPARAR ORIGEM E DESTINO ---
    # Junta os dois dataframes usando o código da CID como chave
    df_merged = pd.merge(
        df_origem,
        df_destino,
        on='CD_CID',
        how='left',
        suffixes=('_novo', '_atual'),
        indicator=True # Adiciona uma coluna '_merge' para identificar a origem de cada linha
    )

    # --- 4. EXECUTAR OPERAÇÕES SEPARADAS ---

    # INSERT: Filtra CIDs que só existem na origem (JSON)
    df_para_inserir = df_merged[df_merged['_merge'] == 'left_only']
    
    # Seleciona apenas as colunas que a tabela espera
    colunas_db = ['CD_CID', 'CD_CID_LINHA', 'CD_CID_CAUSA', 'DS_CID_novo', 'DT_CARGA']
    df_para_inserir = df_para_inserir[colunas_db].rename(columns={'DS_CID_novo': 'DS_CID'}) # Renomeia para o nome correto da coluna
    
    if not df_para_inserir.empty:
        print(f"Inserindo {len(df_para_inserir)} novos registros...")
        # Usa to_sql com append. O DB cuidará da SK_CID (garanta que a tabela tenha AUTOINCREMENT)
        df_para_inserir.to_sql(table_name, conn, if_exists="append", index=False)
    else:
        print("Nenhum registro novo para inserir.")

    # UPDATE: Filtra CIDs que existem em ambos, mas o nome mudou
    df_para_atualizar = df_merged[df_merged['_merge'] == 'both']
    df_para_atualizar = df_para_atualizar[df_para_atualizar['DS_CID_novo'] != df_para_atualizar['DS_CID_atual']]

    if not df_para_atualizar.empty:
        print(f"Atualizando {len(df_para_atualizar)} registros existentes...")
        # Prepara dados para executemany, que é mais performático que um loop
        dados_update = df_para_atualizar[['DS_CID_novo', 'DT_CARGA', 'CD_CID']].to_records(index=False).tolist()
        
        sql_update = f'UPDATE {table_name} SET DS_CID = ?, DT_CARGA = ? WHERE CD_CID = ?'
        cursor.executemany(sql_update, dados_update)
    else:
        print("Nenhum registro para atualizar.")

    conn.commit()
    print("\nCarga merge concluída com sucesso!")

except sqlite3.Error as e:
    print(f"Erro de banco de dados: {e}")
    if conn:
        conn.rollback()
finally:
    if conn:
        cursor.execute(f"INSERT OR IGNORE INTO {table_name} (SK_CID, CD_CID, CD_CID_LINHA, CD_CID_CAUSA, DS_CID, DT_CARGA) VALUES (-1, -1, -1, -1, 'Não Informado', '28-11-1970 00:00')")
        conn.commit()
        conn.close()