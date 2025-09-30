import sqlite3

# --- config ---
dm_path = 'DM.db'

# --- conexao ---
conn_dm = sqlite3.connect(dm_path)
cursor_dm = conn_dm.cursor()

def criar_dm():
    """cria as tabelas do modelo dimensional no banco de destino."""
    print("Criando tabelas do Modelo Dimensional...")

    script_sql = """
        CREATE TABLE IF NOT EXISTS "DIME_TEMPO_ANO" (
            "SK_TEMPO_ANO" INTEGER NOT NULL UNIQUE, "ANO" INTEGER NOT NULL,
            "DT_CARGA" DATETIME NOT NULL, PRIMARY KEY("SK_TEMPO_ANO")
        );
        CREATE TABLE IF NOT EXISTS "DIME_CIRCOBITO" (
            "SK_CIRCOBITO" INTEGER NOT NULL UNIQUE, "CD_CIRCOBITO" INTEGER NOT NULL,
            "DS_CIRCOBITO" VARCHAR NOT NULL, "DT_CARGA" DATETIME NOT NULL, PRIMARY KEY("SK_CIRCOBITO")
        );
        CREATE TABLE IF NOT EXISTS "DIME_TPLOCOR" (
            "SK_TPLOCOR" INTEGER NOT NULL UNIQUE, "CD_TPLOCOR" INTEGER NOT NULL,
            "DS_TPLOCOR" VARCHAR NOT NULL, "DT_CARGA" DATETIME NOT NULL, PRIMARY KEY("SK_TPLOCOR")
        );
        CREATE TABLE IF NOT EXISTS "DIME_PESSOA" (
            "SK_PESSOA" INTEGER NOT NULL UNIQUE, "CD_SEXO" INTEGER NOT NULL, "DS_SEXO" VARCHAR NOT NULL,
            "CD_RACACOR" INTEGER NOT NULL, "DS_RACACOR" VARCHAR NOT NULL,
            "CD_ESTCIV" INTEGER NOT NULL, "DS_ESTCIV" VARCHAR NOT NULL,
            "CD_OCUP" INTEGER NOT NULL, "DS_OCUP" VARCHAR NOT NULL,
            "CD_ESCFAL" INTEGER NOT NULL, "DS_ESCFAL" VARCHAR NOT NULL,
            "DT_NASC" DATE NOT NULL, "DT_CARGA" DATETIME NOT NULL, PRIMARY KEY("SK_PESSOA")
        );
        CREATE TABLE IF NOT EXISTS "DIME_TEMPO_DIA" (
            "SK_TEMPO_DIA" INTEGER NOT NULL UNIQUE, "DATA" DATE NOT NULL, "DIA" VARCHAR NOT NULL,
            "MES" VARCHAR NOT NULL, "ANO" VARCHAR NOT NULL, "ST_FERIADO" BOOLEAN NOT NULL,
            "DT_CARGA" DATETIME NOT NULL, PRIMARY KEY("SK_TEMPO_DIA")
        );
        CREATE TABLE IF NOT EXISTS "DIME_CAUSA" (
            "SK_CAUSA" INTEGER NOT NULL UNIQUE, "CD_CID_BASICA" VARCHAR NOT NULL,
            "DS_CID_BASICA" VARCHAR NOT NULL, "CD_CID_TERMINAL" VARCHAR NOT NULL,
            "DS_CID_TERMINAL" VARCHAR NOT NULL, "DT_CARGA" DATETIME NOT NULL, PRIMARY KEY("SK_CAUSA")
        );
        CREATE TABLE IF NOT EXISTS "DIME_LOCAL" (
            "SK_LOCAL" INTEGER NOT NULL UNIQUE, "CD_MUNICIPIO" INTEGER NOT NULL, "NM_MUNICIPIO" VARCHAR NOT NULL,
            "ST_CAPITAL" BOOLEAN NOT NULL, "CD_ESTADO" INTEGER NOT NULL, "NM_ESTADO" VARCHAR NOT NULL,
            "CD_REGIAO" INTEGER NOT NULL, "NM_REGIAO" VARCHAR NOT NULL,
            "DT_CARGA" DATETIME NOT NULL, PRIMARY KEY("SK_LOCAL")
        );
        CREATE TABLE IF NOT EXISTS "FATO_OBITO" (
            "CD_OBITO" INTEGER NOT NULL, "SK_PESSOA" INTEGER NOT NULL,
            "SK_LOCAL_OBITO" INTEGER NOT NULL, "SK_LOCAL_NASC" INTEGER NOT NULL,
            "SK_DT_OBITO" INTEGER NOT NULL, "SK_CAUSA" INTEGER NOT NULL,
            "SK_CIRCOBITO" INTEGER NOT NULL, "SK_TPLOCOR" INTEGER NOT NULL,
            "ST_FETAL" BOOLEAN NOT NULL, "ST_ACIDTRAB" BOOLEAN NOT NULL,
            "QTD_OBITO" INTEGER NOT NULL, "DT_CARGA" DATETIME NOT NULL,
            PRIMARY KEY("CD_OBITO"),
            FOREIGN KEY ("SK_LOCAL_OBITO") REFERENCES "DIME_LOCAL"("SK_LOCAL"),
            FOREIGN KEY ("SK_CAUSA") REFERENCES "DIME_CAUSA"("SK_CAUSA"),
            FOREIGN KEY ("SK_DT_OBITO") REFERENCES "DIME_TEMPO_DIA"("SK_TEMPO_DIA"),
            FOREIGN KEY ("SK_PESSOA") REFERENCES "DIME_PESSOA"("SK_PESSOA"),
            FOREIGN KEY ("SK_CIRCOBITO") REFERENCES "DIME_CIRCOBITO"("SK_CIRCOBITO"),
            FOREIGN KEY ("SK_TPLOCOR") REFERENCES "DIME_TPLOCOR"("SK_TPLOCOR"),
            FOREIGN KEY ("SK_LOCAL_NASC") REFERENCES "DIME_LOCAL"("SK_LOCAL")
        );
        CREATE TABLE IF NOT EXISTS "FATO_POPULACAO" (
            "SK_TEMPO" INTEGER NOT NULL, "SK_LOCAL" INTEGER NOT NULL,
            "QTD_POPULACAO" INTEGER NOT NULL, "QTD_MORTES" INTEGER NOT NULL,
            "DT_CARGA" DATETIME NOT NULL, PRIMARY KEY("SK_TEMPO", "SK_LOCAL"),
            FOREIGN KEY ("SK_TEMPO") REFERENCES "DIME_TEMPO_ANO"("SK_TEMPO_ANO"),
            FOREIGN KEY ("SK_LOCAL") REFERENCES "DIME_LOCAL"("SK_LOCAL")
        );
    """
    cursor_dm.executescript(script_sql)
    conn_dm.commit()
    print("Tabelas criadas com sucesso.")

def truncar_dm():
    """Remove todos os registros das tabelas do DM (carga full)."""
    print("Truncando tabelas do Modelo Dimensional...")
    tabelas = [
        "FATO_OBITO",
        "FATO_POPULACAO",
        "DIME_TEMPO_ANO",
        "DIME_CIRCOBITO",
        "DIME_TPLOCOR",
        "DIME_PESSOA",
        "DIME_TEMPO_DIA",
        "DIME_CAUSA",
        "DIME_LOCAL"
    ]
    for tabela in tabelas:
        cursor_dm.execute(f"DELETE FROM {tabela};")
    conn_dm.commit()
    print("Tabelas truncadas com sucesso.")

if __name__ == '__main__':
    try:
        criar_dm()
        truncar_dm()
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
    finally:
        if conn_dm:
            conn_dm.close()