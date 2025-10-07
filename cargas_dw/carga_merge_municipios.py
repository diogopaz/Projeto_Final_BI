import pandas as pd
from datetime import datetime, timezone, timedelta
import sqlite3
import sys
import os


def carregar_dimensoes_geograficas(caminho_csv_ibge, caminho_csv_municipios, db_path):
    """
    Processa arquivos CSV do IBGE e de municípios (com flag de capital)
    e carrega as dimensões de Região, Estado e Município em um banco SQLite.
    """

    # --- 1. LEITURA DOS ARQUIVOS ---
    try:
        ibge_municipio = pd.read_csv(caminho_csv_ibge, sep=";")
        df_municipios = pd.read_csv(caminho_csv_municipios, sep=",")
    except Exception as e:
        print(f"Erro ao ler os arquivos: {e}")
        return

    # --- 2. MAPA DE REGIÕES ---
    regioes = ["Norte", "Nordeste", "Centro-Oeste", "Sudeste", "Sul"]
    mapa_regiao = {
        "Rondônia": "Norte", "Acre": "Norte", "Amazonas": "Norte", "Roraima": "Norte",
        "Pará": "Norte", "Amapá": "Norte", "Tocantins": "Norte",
        "Maranhão": "Nordeste", "Piauí": "Nordeste", "Ceará": "Nordeste", "Rio Grande do Norte": "Nordeste",
        "Paraíba": "Nordeste", "Pernambuco": "Nordeste", "Alagoas": "Nordeste", "Sergipe": "Nordeste", "Bahia": "Nordeste",
        "Minas Gerais": "Sudeste", "Espírito Santo": "Sudeste", "Rio de Janeiro": "Sudeste", "São Paulo": "Sudeste",
        "Paraná": "Sul", "Santa Catarina": "Sul", "Rio Grande do Sul": "Sul",
        "Mato Grosso do Sul": "Centro-Oeste", "Mato Grosso": "Centro-Oeste",
        "Goiás": "Centro-Oeste", "Distrito Federal": "Centro-Oeste"
    }

    # --- 3. DATA DE CARGA ---
    br_tz = timezone(timedelta(hours=-3))
    data_carga = datetime.now(br_tz).strftime("%d-%m-%Y %H:%M")

    # --- 4. REGIÃO ---
    df_regiao = pd.DataFrame({
        "SK_REGIAO": range(1, len(regioes) + 1),
        "NM_REGIAO": regioes,
        "DT_CARGA": data_carga
    })

    # --- 5. ESTADO ---
    df_estado = ibge_municipio.rename(columns={
        "Nome_UF": "NM_ESTADO",
        "UF": "CD_ESTADO"
    })
    df_estado = df_estado[["NM_ESTADO", "CD_ESTADO"]].drop_duplicates().sort_values("NM_ESTADO").reset_index(drop=True)
    df_estado["SK_ESTADO"] = range(1, len(df_estado) + 1)
    df_estado["NM_REGIAO"] = df_estado["NM_ESTADO"].map(mapa_regiao)
    df_estado = df_estado.merge(df_regiao[["SK_REGIAO", "NM_REGIAO"]], on="NM_REGIAO", how="left")
    df_estado["DT_CARGA"] = data_carga
    df_estado = df_estado[["SK_ESTADO", "CD_ESTADO", "NM_ESTADO", "SK_REGIAO", "DT_CARGA"]]

    # --- 6. MUNICÍPIO ---
    ibge_municipio = ibge_municipio.rename(columns={
        "Código Município Completo": "CD_MUNICIPIO",
        "Nome_Município": "NM_MUNICIPIO",
        "UF": "CD_ESTADO"
    })
    ibge_municipio["SK_MUNICIPIO"] = range(1, len(ibge_municipio) + 1)
    ibge_municipio["CD_MUNICIPIO"] = ibge_municipio["CD_MUNICIPIO"].astype(str).str[:6].astype(int)
    df_municipios['codigo_ibge'] = df_municipios['codigo_ibge'].astype(str).str[:6].astype(int)

    ibge_municipio = ibge_municipio.merge(
        df_estado[["SK_ESTADO", "CD_ESTADO"]],
        on="CD_ESTADO",
        how="left"
    )

    # Função para identificar capital
    def is_capital(cd_municipio):
        cidade = df_municipios[df_municipios['codigo_ibge'] == cd_municipio]
        if cidade.empty:
            return 0
        return int(cidade['capital'].values[0] == 1)

    ibge_municipio['ST_CAPITAL'] = ibge_municipio['CD_MUNICIPIO'].apply(is_capital)
    ibge_municipio["DT_CARGA"] = data_carga

    df_municipio = ibge_municipio[[
        "SK_MUNICIPIO", "CD_MUNICIPIO", "SK_ESTADO", "NM_MUNICIPIO", "ST_CAPITAL", "DT_CARGA"
    ]]

    # --- 7. CARGA NO BANCO ---
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        dados_regiao = df_regiao.to_records(index=False).tolist()
        dados_estado = df_estado.to_records(index=False).tolist()
        dados_municipio = df_municipio.to_records(index=False).tolist()

        sql_regiao = """
            INSERT INTO DWCD_REGIAO (SK_REGIAO, NM_REGIAO, DT_CARGA)
            VALUES (?, ?, ?)
            ON CONFLICT(SK_REGIAO) DO UPDATE SET NM_REGIAO=excluded.NM_REGIAO,
            DT_CARGA=excluded.DT_CARGA;
        """
        cursor.executemany(sql_regiao, dados_regiao)

        sql_estado = """
            INSERT INTO DWCD_ESTADO (SK_ESTADO, CD_ESTADO, NM_ESTADO, SK_REGIAO, DT_CARGA)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(SK_ESTADO) DO UPDATE SET CD_ESTADO=excluded.CD_ESTADO,
            NM_ESTADO=excluded.NM_ESTADO, SK_REGIAO=excluded.SK_REGIAO,
            DT_CARGA=excluded.DT_CARGA;
        """
        cursor.executemany(sql_estado, dados_estado)

        sql_municipio = """
            INSERT INTO DWCD_MUNICIPIO (SK_MUNICIPIO, CD_MUNICIPIO, SK_ESTADO, NM_MUNICIPIO, ST_CAPITAL, DT_CARGA)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(SK_MUNICIPIO) DO UPDATE SET CD_MUNICIPIO=excluded.CD_MUNICIPIO,
            SK_ESTADO=excluded.SK_ESTADO, NM_MUNICIPIO=excluded.NM_MUNICIPIO,
            ST_CAPITAL=excluded.ST_CAPITAL, DT_CARGA=excluded.DT_CARGA;
        """
        cursor.executemany(sql_municipio, dados_municipio)

        # Registros "Não Informado"
        cursor.execute("INSERT OR IGNORE INTO DWCD_REGIAO VALUES (-1, 'Não Informado', ?)", (data_carga,))
        cursor.execute("INSERT OR IGNORE INTO DWCD_ESTADO VALUES (-1, -1, 'Não Informado', -1, ?)", (data_carga,))
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
        print("Uso: python carregar_dimensoes.py <caminho_csv_ibge> <caminho_csv_municipios>")
        sys.exit(1)

    caminho_csv_ibge = sys.argv[1]
    caminho_csv_municipios = sys.argv[2]
    banco_de_dados = "DW.db"

    if not os.path.exists(caminho_csv_ibge):
        print(f"Erro: CSV IBGE '{caminho_csv_ibge}' não encontrado.")
        sys.exit(1)

    if not os.path.exists(caminho_csv_municipios):
        print(f"Erro: CSV municípios '{caminho_csv_municipios}' não encontrado.")
        sys.exit(1)

    carregar_dimensoes_geograficas(caminho_csv_ibge, caminho_csv_municipios, banco_de_dados)
