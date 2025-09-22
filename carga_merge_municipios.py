import pandas as pd
from datetime import datetime, timezone, timedelta
import sqlite3
import sys
import os

def carregar_dimensoes_geograficas(caminho_csv, db_path):
    """
    Processa um arquivo CSV do IBGE e carrega as dimensões de Região,
    Estado e Município em um banco de dados SQLite.
    """
    # --- 1. LEITURA E VALIDAÇÃO DO ARQUIVO DE ORIGEM ---
    try:
        ibge_municipio = pd.read_csv(caminho_csv, sep=';')
        # Valida se as colunas esperadas existem
        colunas_necessarias = [
            'Nome_UF', 'UF', 'Código Município Completo', 'Nome_Município'
        ]
        if not all(col in ibge_municipio.columns for col in colunas_necessarias):
            print("Erro: O arquivo CSV de entrada não contém as colunas necessárias.")
            return
    except FileNotFoundError:
        print(f"Erro: O arquivo '{caminho_csv}' não foi encontrado.")
        return
    except Exception as e:
        print(f"Erro ao ler o arquivo CSV: {e}")
        return

    # --- 2. PREPARAÇÃO DOS DADOS ---
    br_tz = timezone(timedelta(hours=-3))
    data_carga = datetime.now(br_tz).strftime('%Y-%m-%d %H:%M:%S')

    # DataFrame de Regiões
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

    # DataFrame de Estados
    df_estado = ibge_municipio[['Nome_UF', 'UF']].drop_duplicates().sort_values('Nome_UF').reset_index(drop=True)
    df_estado.rename(columns={'Nome_UF': 'NM_ESTADO', 'UF': 'CD_ESTADO'}, inplace=True)
    df_estado['SK_ESTADO'] = range(1, len(df_estado) + 1)
    df_estado['NM_REGIAO'] = df_estado['NM_ESTADO'].map(mapa_regiao)
    df_estado = pd.merge(df_estado, df_regiao[['SK_REGIAO', 'NM_REGIAO']], on='NM_REGIAO', how='left')
    df_estado['DT_CARGA'] = data_carga
    df_estado = df_estado[['SK_ESTADO', 'CD_ESTADO', 'NM_ESTADO', 'SK_REGIAO', 'DT_CARGA']]

    # DataFrame de Municípios
    df_municipio = ibge_municipio.rename(columns={
        'Código Município Completo': 'CD_MUNICIPIO',
        'Nome_Município': 'NM_MUNICIPIO',
        'UF': 'CD_ESTADO'
    })
    df_municipio['SK_MUNICIPIO'] = range(1, len(df_municipio) + 1)
    df_municipio = pd.merge(df_municipio, df_estado[['SK_ESTADO', 'CD_ESTADO']], on='CD_ESTADO', how='left')
    df_municipio['CD_MUNICIPIO'] = df_municipio['CD_MUNICIPIO'].astype(str).str[:6].astype(int)
    df_municipio['DT_CARGA'] = data_carga
    df_municipio = df_municipio[['SK_MUNICIPIO', 'CD_MUNICIPIO', 'SK_ESTADO', 'NM_MUNICIPIO', 'DT_CARGA']]

    # --- 3. CARGA NO BANCO DE DADOS ---
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Converte DataFrames para listas de tuplas para inserção otimizada
        dados_regiao = df_regiao.to_records(index=False).tolist()
        dados_estado = df_estado.to_records(index=False).tolist()
        dados_municipio = df_municipio.to_records(index=False).tolist()

        # Otimização: Usar executemany em vez de iterar linha a linha
        sql_regiao = """
            INSERT INTO DWCD_REGIAO (SK_REGIAO, NM_REGIAO, DT_CARGA) VALUES (?, ?, ?)
            ON CONFLICT(SK_REGIAO) DO UPDATE SET NM_REGIAO=excluded.NM_REGIAO, DT_CARGA=excluded.DT_CARGA;
        """
        cursor.executemany(sql_regiao, dados_regiao)

        sql_estado = """
            INSERT INTO DWCD_ESTADO (SK_ESTADO, CD_ESTADO, NM_ESTADO, SK_REGIAO, DT_CARGA) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(SK_ESTADO) DO UPDATE SET CD_ESTADO=excluded.CD_ESTADO, NM_ESTADO=excluded.NM_ESTADO,
            SK_REGIAO=excluded.SK_REGIAO, DT_CARGA=excluded.DT_CARGA;
        """
        cursor.executemany(sql_estado, dados_estado)

        sql_municipio = """
            INSERT INTO DWCD_MUNICIPIO (SK_MUNICIPIO, CD_MUNICIPIO, SK_ESTADO, NM_MUNICIPIO, DT_CARGA) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(SK_MUNICIPIO) DO UPDATE SET CD_MUNICIPIO=excluded.CD_MUNICIPIO, SK_ESTADO=excluded.SK_ESTADO,
            NM_MUNICIPIO=excluded.NM_MUNICIPIO, DT_CARGA=excluded.DT_CARGA;
        """
        cursor.executemany(sql_municipio, dados_municipio)

        # Inserção do registro "Não Informado" para cada dimensão
        cursor.execute("INSERT OR IGNORE INTO DWCD_REGIAO VALUES (-1, 'Não Informado', ?)", (data_carga,))
        cursor.execute("INSERT OR IGNORE INTO DWCD_ESTADO VALUES (-1, -1, 'Não Informado', -1, ?)", (data_carga,))
        cursor.execute("INSERT OR IGNORE INTO DWCD_MUNICIPIO VALUES (-1, -1, -1, 'Não Informado', ?)", (data_carga,))
        
        conn.commit()
        print("Carga concluída com sucesso!")

    except sqlite3.Error as e:
        print(f"Erro de banco de dados: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

# --- PONTO DE ENTRADA DO SCRIPT ---
if __name__ == "__main__":
    # Verifica se o caminho do arquivo foi passado como argumento
    if len(sys.argv) != 2:
        print("Erro: Forneça o caminho para o arquivo CSV como um argumento.")
        print(f"Uso: python {os.path.basename(__file__)} <caminho_do_arquivo.csv>")
        sys.exit(1)

    caminho_do_csv = sys.argv[1]
    banco_de_dados = 'DW.db'
    
    # Verifica se o arquivo realmente existe antes de chamar a função
    if not os.path.exists(caminho_do_csv):
        print(f"Erro: O arquivo '{caminho_do_csv}' não existe ou o caminho está incorreto.")
        sys.exit(1)

    carregar_dimensoes_geograficas(caminho_do_csv, banco_de_dados)