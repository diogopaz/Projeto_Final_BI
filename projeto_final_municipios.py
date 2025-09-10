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
ibge_regiao['NM_Regiao'] = regiao

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

ibge_municipio.rename(columns={"SK_ESTADO": "CD_ESTADO"}, inplace=True)

ibge_estado["NM_REGIAO"] = ibge_estado["NM_ESTADO"].map(mapa_regiao)

ibge_estado = ibge_estado.merge(
    ibge_regiao,
    left_on="NM_REGIAO",
    right_on="NM_Regiao",
    how="left"
)

ibge_estado.rename(columns={"SK_REGIAO": "CD_REGIAO"}, inplace=True)
ibge_estado = ibge_estado[["SK_ESTADO", "CD_ESTADO", "NM_ESTADO", "CD_REGIAO", "DT_CARGA"]]

con = sqlite3.connect('DW.db')
ibge_regiao.to_sql('DWCD_REGIAO', con, if_exists="replace", index=False)
ibge_municipio.to_sql('DWCD_MUNICIPIO', con, if_exists="replace", index=False)
ibge_estado.to_sql('DWCD_ESTADO', con, if_exists="replace", index=False)
con.close()