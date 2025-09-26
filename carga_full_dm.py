import subprocess
path_drive = '/content/drive/MyDrive/projeto_final_BI'

subprocess.run(["python", "cargas_dm/create_dm.py"], check=True)
subprocess.run(["python", "cargas_dm/carga_dime_tempo_ano.py"], check=True)
subprocess.run(["python", "cargas_dm/carga_dime_circobito.py"], check=True)
subprocess.run(["python", "cargas_dm/carga_dime_tplocor.py"], check=True)
subprocess.run(["python", "cargas_dm/carga_dime_local.py"], check=True)
subprocess.run(["python", "cargas_dm/carga_dime_pessoa.py"], check=True)
subprocess.run(["python", "cargas_dm/carga_dime_causa.py"], check=True)
subprocess.run(["python", "cargas_dm/carga_dime_tempo_dia.py"], check=True)
subprocess.run(["python", "cargas_dm/carga_fato_populacao.py"], check=True)
subprocess.run(["python", "cargas_dm/carga_fato_obito.py"], check=True)
