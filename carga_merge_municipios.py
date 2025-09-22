import pandas as pd
from datetime import datetime, timezone, timedelta
import sqlite3
import sys
import os

def carregar_dimensoes_geograficas(caminho_csv, caminho_populacao, db_path):
    """
    Processa um arquivo CSV do IBGE e uma planilha de população e carrega as dimensões de Região,
    Estado e Município em um banco de dados SQLite.
    """
    # --- 1. LEITURA E VALIDAÇÃO DO ARQUIVO DE ORIGEM ---  
    try:
        ibge_municipio = pd.read_csv(caminho_csv, sep=';')
        colunas_necessarias = ['Nome_UF', 'UF', 'Código Município Completo', 'Nome_Município']
        if not all(col in ibge_municipio.columns for col in colunas_necessarias):
            print("Erro: O arquivo CSV de entrada não contém as colunas necessárias.")
            return
    except FileNotFoundError:
        print(f"Erro: O arquivo '{caminho_csv}' não foi encontrado.")
        return
    except Exception as e:
        print(f"Erro ao ler o arquivo CSV: {e}")
        return

    # --- 1.1. LEITURA DA POPULAÇÃO POR MUNICÍPIO ---  
    try:
        populacao = pd.read_excel(caminho_populacao, sheet_name=1, header=1)
        populacao = populacao.dropna()
        populacao['COD. UF'] = populacao['COD. UF'].astype(int).astype(str)
        populacao['COD. MUNIC'] = populacao['COD. MUNIC'].astype(int).astype(str).str.zfill(5)
        populacao['CD_MUNICIPIO'] = (populacao['COD. UF'] + populacao['COD. MUNIC']).str[:6].astype(int)
        populacao = populacao.rename(columns={'POPULAÇÃO ESTIMADA': 'QTD_POPULACAO'})
        populacao_tratado = populacao[['CD_MUNICIPIO', 'QTD_POPULACAO']]
    except Exception as e:
        print(f"Erro ao ler o arquivo de população por município: {e}")
        return

    # --- 1.2. LEITURA DA POPULAÇÃO POR ESTADO/REGIÃO ---  
    try:
        populacao_estado = pd.read_excel(caminho_populacao, sheet_name=0, header=1)
        populacao_estado_tratado = populacao_estado[['BRASIL E UNIDADES DA FEDERAÇÃO', 'POPULAÇÃO ESTIMADA']]
        populacao_estado_tratado = populacao_estado_tratado.rename(
            columns={
                'BRASIL E UNIDADES DA FEDERAÇÃO': 'NM_ESTADO_REGIAO',
                'POPULAÇÃO ESTIMADA': 'QTD_POPULACAO'
            }
        )
        populacao_estado_tratado = populacao_estado_tratado.dropna()
        populacao_estado_tratado['QTD_POPULACAO'] = populacao_estado_tratado['QTD_POPULACAO'].astype(int)
    except Exception as e:
        print(f"Erro ao ler o arquivo de população por estado/região: {e}")
        return

    # --- 2. ADICIONA POPULAÇÃO AO IBGE_MUNICIPIO ---  
    ibge_municipio['Código Município Completo'] = ibge_municipio['Código Município Completo'].astype(str).str[:6].astype(int)
    ibge_municipio = ibge_municipio.merge(
        populacao_tratado,
        left_on='Código Município Completo',
        right_on='CD_MUNICIPIO',
        how='left'
    )
    ibge_municipio['QTD_POPULACAO'] = ibge_municipio['QTD_POPULACAO'].fillna(0).astype(int)

    # --- 3. PREPARAÇÃO DOS DADOS ---  
    br_tz = timezone(timedelta(hours=-3))
    data_carga = datetime.now(br_tz).strftime('%Y-%m-%d %H:%M:%S')

    # --- 3.1. REGIÕES ---  
    regioes_lista = ['Norte', 'Nordeste', 'Centro-Oeste', 'Sudeste', 'Sul']
    df_regiao = pd.DataFrame({
        'SK_REGIAO': range(1, len(regioes_lista) + 1),
        'NM_REGIAO': regioes_lista,
        'DT_CARGA': data_carga
    })

    mapa_regiao = {
        "Rondônia": "Norte", "Acre": "Norte", "Amazonas": "Norte", "Roraima": "Norte",
        "Pará": "Norte", "Amapá": "Norte", "Tocantins": "Norte", "Maranhão": "Nordeste",
        "Piauí": "Nordeste", "Ceará": "Nordeste", "Rio Grande do Norte": "Nordeste",
        "Paraíba": "Nordeste", "Pernambuco": "Nordeste", "Alagoas": "Nordeste",
        "Sergipe": "Nordeste", "Bahia": "Nordeste", "Minas Gerais": "Sudeste",
        "Espírito Santo": "Sudeste", "Rio de Janeiro": "Sudeste", "São Paulo": "Sudeste",
        "Paraná": "Sul", "Santa Catarina": "Sul", "Rio Grande do Sul": "Sul",
        "Mato Grosso do Sul": "Centro-Oeste", "Mato Grosso": "Centro-Oeste",
        "Goiás": "Centro-Oeste", "Distrito Federal": "Centro-Oeste"
    }

    # --- 3.2. ESTADOS ---  
    df_estado = ibge_municipio[['Nome_UF', 'UF']].drop_duplicates().sort_values('Nome_UF').reset_index(drop=True)
    df_estado.rename(columns={'Nome_UF': 'NM_ESTADO', 'UF': 'CD_ESTADO'}, inplace=True)
    df_estado['SK_ESTADO'] = range(1, len(df_estado) + 1)
    df_estado['NM_REGIAO'] = df_estado['NM_ESTADO'].map(mapa_regiao)
    df_estado = pd.merge(df_estado, df_regiao[['SK_REGIAO', 'NM_REGIAO']], on='NM_REGIAO', how='left')
    df_estado['DT_CARGA'] = data_carga

    # --- 3.3. ADICIONA POPULAÇÃO POR ESTADO ---  
    df_estado = df_estado.merge(
        populacao_estado_tratado[['NM_ESTADO_REGIAO', 'QTD_POPULACAO']],
        left_on='NM_ESTADO',
        right_on='NM_ESTADO_REGIAO',
        how='inner'
    )
    df_estado = df_estado[['SK_ESTADO', 'CD_ESTADO', 'NM_ESTADO', 'QTD_POPULACAO', 'SK_REGIAO', 'DT_CARGA']]

    # --- 3.4. POPULAÇÃO POR REGIÃO ---  
    df_regiao = df_regiao.merge(
        populacao_estado_tratado[['NM_ESTADO_REGIAO', 'QTD_POPULACAO']],
        left_on='NM_REGIAO',
        right_on='NM_ESTADO_REGIAO',
        how='inner'
    )
    df_regiao = df_regiao[['SK_REGIAO', 'NM_REGIAO', 'QTD_POPULACAO', 'DT_CARGA']]

    # --- 3.5. MUNICÍPIOS ---  
    df_municipio = ibge_municipio.rename(columns={
        'Código Município Completo': 'CD_MUNICIPIO',
        'Nome_Município': 'NM_MUNICIPIO',
        'UF': 'CD_ESTADO'
    })
    df_municipio['SK_MUNICIPIO'] = range(1, len(df_municipio) + 1)
    df_municipio = pd.merge(df_municipio, df_estado[['SK_ESTADO', 'CD_ESTADO']], 
                            left_on='CD_ESTADO', right_on='CD_ESTADO', how='left')
    df_municipio['DT_CARGA'] = data_carga
    df_municipio = df_municipio.loc[:, ~df_municipio.columns.duplicated()]
    df_municipio = df_municipio[['SK_MUNICIPIO', 'CD_MUNICIPIO', 'SK_ESTADO', 'NM_MUNICIPIO', 'QTD_POPULACAO', 'DT_CARGA']]

    # --- 4. CARGA NO BANCO DE DADOS ---  
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        dados_regiao = df_regiao.to_records(index=False).tolist()
        dados_estado = df_estado.to_records(index=False).tolist()
        dados_municipio = df_municipio.to_records(index=False).tolist()

        sql_regiao = """
            INSERT INTO DWCD_REGIAO (SK_REGIAO, NM_REGIAO, QTD_POPULACAO, DT_CARGA)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(SK_REGIAO) DO UPDATE SET NM_REGIAO=excluded.NM_REGIAO,
            QTD_POPULACAO=excluded.QTD_POPULACAO, DT_CARGA=excluded.DT_CARGA;
        """
        cursor.executemany(sql_regiao, dados_regiao)

        sql_estado = """
            INSERT INTO DWCD_ESTADO (SK_ESTADO, CD_ESTADO, NM_ESTADO, QTD_POPULACAO, SK_REGIAO, DT_CARGA)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(SK_ESTADO) DO UPDATE SET CD_ESTADO=excluded.CD_ESTADO,
            NM_ESTADO=excluded.NM_ESTADO, QTD_POPULACAO=excluded.QTD_POPULACAO,
            SK_REGIAO=excluded.SK_REGIAO, DT_CARGA=excluded.DT_CARGA;
        """
        cursor.executemany(sql_estado, dados_estado)

        sql_municipio = """
            INSERT INTO DWCD_MUNICIPIO (SK_MUNICIPIO, CD_MUNICIPIO, SK_ESTADO, NM_MUNICIPIO, QTD_POPULACAO, DT_CARGA)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(SK_MUNICIPIO) DO UPDATE SET CD_MUNICIPIO=excluded.CD_MUNICIPIO,
            SK_ESTADO=excluded.SK_ESTADO, NM_MUNICIPIO=excluded.NM_MUNICIPIO,
            QTD_POPULACAO=excluded.QTD_POPULACAO, DT_CARGA=excluded.DT_CARGA;
        """
        cursor.executemany(sql_municipio, dados_municipio)

        # Inserção do registro "Não Informado"
        cursor.execute("INSERT OR IGNORE INTO DWCD_REGIAO VALUES (-1, 'Não Informado', 0, ?)", (data_carga,))
        cursor.execute("INSERT OR IGNORE INTO DWCD_ESTADO VALUES (-1, -1, 'Não Informado', 0, -1, ?)", (data_carga,))
        cursor.execute("INSERT OR IGNORE INTO DWCD_MUNICIPIO VALUES (-1, -1, -1, 'Não Informado', 0, ?)", (data_carga,))
        
        conn.commit()
        print("Carga concluída com sucesso!")

    except sqlite3.Error as e:
        print(f"Erro de banco de dados: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python carregar_dimensoes.py <caminho_csv_ibge> <caminho_excel_populacao>")
        sys.exit(1)

    caminho_csv = sys.argv[1]
    caminho_excel = sys.argv[2]
    banco_de_dados = "DW.db"

    if not os.path.exists(caminho_csv):
        print(f"Erro: CSV IBGE '{caminho_csv}' não encontrado.")
        sys.exit(1)
    
    if not os.path.exists(caminho_excel):
        print(f"Erro: Excel população '{caminho_excel}' não encontrado.")
        sys.exit(1)

    carregar_dimensoes_geograficas(caminho_csv, caminho_excel, banco_de_dados)
