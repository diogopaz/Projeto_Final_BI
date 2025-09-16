import pandas as pd
from datetime import datetime, timezone, timedelta
import sqlite3

ibge_municipio = pd.read_csv('Municipios_IBGE.csv', sep=';')
ibge_estado = ibge_municipio.copy()

regiao = ['Norte', 'Nordeste', 'Centro-Oeste', 'Sudeste', 'Sul']

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

br_tz = timezone(timedelta(hours=-3))

ibge_regiao = pd.DataFrame()
ibge_regiao['SK_REGIAO'] = range(1, len(regiao) + 1)
ibge_regiao['NM_REGIAO'] = regiao

ibge_estado.rename(columns={
    "Nome_UF": "NM_ESTADO",
    "UF": "CD_ESTADO"
}, inplace=True)

ibge_estado = ibge_estado[["NM_ESTADO", "CD_ESTADO"]]
ibge_estado = ibge_estado.drop_duplicates().sort_values("NM_ESTADO").reset_index(drop=True)


ibge_estado["SK_ESTADO"] = range(1, len(ibge_estado) + 1)


ibge_estado["DT_CARGA"] = datetime.now(br_tz).strftime("%d-%m-%Y %H:%M")

ibge_estado = ibge_estado[["SK_ESTADO", "CD_ESTADO", "NM_ESTADO", "DT_CARGA"]]

ibge_municipio.rename(columns={
    "Código Município Completo": "CD_MUNICIPIO",
    "Nome_Município": "NM_MUNICIPIO",
    "UF": "CD_ESTADO"
}, inplace=True)

ibge_municipio["SK_MUNICIPIO"] = range(1, len(ibge_municipio) + 1)

ibge_municipio = ibge_municipio.merge(
    ibge_estado[["SK_ESTADO", "CD_ESTADO"]],
    on="CD_ESTADO",
    how="left"
)

ibge_municipio['CD_MUNICIPIO'] = (
    ibge_municipio['CD_MUNICIPIO']
    .astype(str)
    .str[:6]
    .astype(int)
)
ibge_municipio["DT_CARGA"] = datetime.now(br_tz).strftime("%d-%m-%Y %H:%M")
ibge_municipio = ibge_municipio[[
    "SK_MUNICIPIO", "CD_MUNICIPIO", "SK_ESTADO", "NM_MUNICIPIO", "DT_CARGA"
]]

ibge_estado["NM_REGIAO"] = ibge_estado["NM_ESTADO"].map(mapa_regiao)

ibge_estado = ibge_estado.merge(
    ibge_regiao,
    left_on="NM_REGIAO",
    right_on="NM_REGIAO",
    how="left"
)
ibge_regiao["DT_CARGA"] = datetime.now(br_tz).strftime("%d-%m-%Y %H:%M")

ibge_estado = ibge_estado[["SK_ESTADO", "CD_ESTADO", "NM_ESTADO", "SK_REGIAO", "DT_CARGA"]]

con = sqlite3.connect('DW.db')
cur = con.cursor()
for _, row in ibge_regiao.iterrows():
    cur.execute("""
        INSERT INTO DWCD_REGIAO (SK_REGIAO, NM_REGIAO, DT_CARGA)
        VALUES (?, ?, ?)
        ON CONFLICT(SK_REGIAO) DO UPDATE SET
            NM_REGIAO = excluded.NM_REGIAO,
            DT_CARGA = excluded.DT_CARGA;
    """, tuple(row))

for _, row in ibge_estado.iterrows():
    cur.execute("""
        INSERT INTO DWCD_ESTADO (SK_ESTADO, CD_ESTADO, NM_ESTADO, SK_REGIAO, DT_CARGA)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(SK_ESTADO) DO UPDATE SET
            CD_ESTADO = excluded.CD_ESTADO,
            NM_ESTADO = excluded.NM_ESTADO,
            SK_REGIAO = excluded.SK_REGIAO,
            DT_CARGA = excluded.DT_CARGA;
    """, tuple(row))

for _, row in ibge_municipio.iterrows():
    cur.execute("""
        INSERT INTO DWCD_MUNICIPIO (SK_MUNICIPIO, CD_MUNICIPIO, SK_ESTADO, NM_MUNICIPIO, DT_CARGA)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(SK_MUNICIPIO) DO UPDATE SET
            CD_MUNICIPIO = excluded.CD_MUNICIPIO,
            SK_ESTADO = excluded.SK_ESTADO,
            NM_MUNICIPIO = excluded.NM_MUNICIPIO,
            DT_CARGA = excluded.DT_CARGA;
    """, tuple(row))

con.commit()

con.close()