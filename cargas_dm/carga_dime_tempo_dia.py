import sqlite3
from datetime import date, datetime, timedelta, timezone
import holidays
import pandas as pd

# --- config ---
dm_path = 'DM.db'

# --- conexao ---
conn_dm = sqlite3.connect(dm_path)
cursor_dm = conn_dm.cursor()

def carregar_dime_tempo_dia():
    br_tz = timezone(timedelta(hours=-3))
    dt_carga = datetime.now(br_tz).strftime("%d-%m-%Y %H:%M")
    print("Carregando DIME_TEMPO_DIA...")
    start_date = date(2018, 1, 1)
    end_date = date(2024, 12, 31)
    
    datas = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    df = pd.DataFrame(datas, columns=["DATA"])
    df["SK_TEMPO_DIA"] = range(1, len(df) + 1)
    df["DATA_OBJ"] = pd.to_datetime(df["DATA"])
    
    df["DIA"] = df["DATA_OBJ"].dt.day.astype(str).str.zfill(2)
    df["MES"] = df["DATA_OBJ"].dt.month.astype(str).str.zfill(2)
    df["ANO"] = df["DATA_OBJ"].dt.year.astype(str)
    
    feriados_br = holidays.Brazil(years=range(2018, 2025))
    feriados_br_datas = pd.to_datetime(list(feriados_br.keys()))
    df["ST_FERIADO"] = df["DATA_OBJ"].isin(feriados_br_datas)
    
    df["DATA"] = df["DATA_OBJ"].dt.strftime("%d-%m-%Y")
    df['DT_CARGA'] = dt_carga

    colunas_tempo = ["SK_TEMPO_DIA", "DATA", "DIA", "MES", "ANO", "ST_FERIADO", "DT_CARGA"]
    dados_para_inserir = df[colunas_tempo].to_records(index=False).tolist()

    sql_upsert_tempo = """
        INSERT INTO DIME_TEMPO_DIA (SK_TEMPO_DIA, DATA, DIA, MES, ANO, ST_FERIADO, DT_CARGA)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(SK_TEMPO_DIA) DO UPDATE SET
            DATA = excluded.DATA, DIA = excluded.DIA, MES = excluded.MES,
            ANO = excluded.ANO, ST_FERIADO = excluded.ST_FERIADO, DT_CARGA = excluded.DT_CARGA;
    """
    cursor_dm.executemany(sql_upsert_tempo, dados_para_inserir)
    conn_dm.commit()
    print("-> Carga de DIME_TEMPO_DIA concluída.")

if __name__ == '__main__':
    try:
        carregar_dime_tempo_dia()
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        conn_dm.rollback()
    finally:
        conn_dm.close()