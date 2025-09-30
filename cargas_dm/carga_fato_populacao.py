import sqlite3
from datetime import datetime, timedelta, timezone
import pandas as pd

# --- config ---
dw_path = 'DW.db'
dm_path = 'DM.db'

# --- conexoes ---
conn_dw = sqlite3.connect(dw_path)
cursor_dw = conn_dw.cursor()
conn_dm = sqlite3.connect(dm_path)
cursor_dm = conn_dm.cursor()

def carregar_fato_populacao():
    br_tz = timezone(timedelta(hours=-3))
    dt_carga = datetime.now(br_tz).strftime("%d-%m-%Y %H:%M")
    print("Carregando FATO_POPULACAO...")
    print("  -> Criando mapa de anos para lookup...")
    cursor_dm.execute("SELECT ANO, SK_TEMPO_ANO FROM DIME_TEMPO_ANO")
    ano_sk_map = {row[0]: row[1] for row in cursor_dm.fetchall()}

    print("  -> Lendo dados de população do DW...")
    df_pop = pd.read_sql("SELECT ANO, SK_MUNICIPIO AS SK_LOCAL, QTD_POPULACAO FROM DWMV_POPULACAO", conn_dw)
    df_pop['SK_TEMPO'] = df_pop['ANO'].map(ano_sk_map)
    df_pop = df_pop[['SK_TEMPO', 'SK_LOCAL', 'QTD_POPULACAO']].dropna(subset=['SK_TEMPO'])
    df_pop['SK_TEMPO'] = df_pop['SK_TEMPO'].astype(int)

    print("  -> Agregando contagem de mortes do DW...")
    sql_mortes = """
        SELECT 
            CAST(SUBSTR(DT_OBITO, 7, 4) AS INTEGER) as ANO,
            SK_MUNICIPIO AS SK_LOCAL,
            COUNT(*) AS QTD_MORTES
        FROM DWMV_OBITO
        WHERE DT_OBITO IS NOT NULL AND DT_OBITO != '-1'
        GROUP BY ANO, SK_LOCAL
    """
    df_mortes = pd.read_sql(sql_mortes, conn_dw)
    df_mortes['SK_TEMPO'] = df_mortes['ANO'].map(ano_sk_map)
    df_mortes = df_mortes[['SK_TEMPO', 'SK_LOCAL', 'QTD_MORTES']].dropna(subset=['SK_TEMPO'])
    df_mortes['SK_TEMPO'] = df_mortes['SK_TEMPO'].astype(int)

    print("  -> Juntando dados de população e mortes...")
    df_final = pd.merge(df_pop, df_mortes, on=['SK_TEMPO', 'SK_LOCAL'], how='left')
    df_final['QTD_MORTES'] = df_final['QTD_MORTES'].fillna(0).astype(int)
    df_final['DT_CARGA'] = dt_carga

    dados_para_inserir_pop = df_final[['SK_TEMPO', 'SK_LOCAL', 'QTD_POPULACAO', 'QTD_MORTES', 'DT_CARGA']].to_records(index=False).tolist()
    cursor_dm.executemany("""
        INSERT INTO FATO_POPULACAO (SK_TEMPO, SK_LOCAL, QTD_POPULACAO, QTD_MORTES, DT_CARGA)
        VALUES (?, ?, ?, ?, ?)
    """, dados_para_inserir_pop)
    conn_dm.commit()
    print("-> Carga de FATO_POPULACAO concluída.")

if __name__ == '__main__':
    try:
        carregar_fato_populacao()
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        conn_dm.rollback()
    finally:
        conn_dw.close()
        conn_dm.close()