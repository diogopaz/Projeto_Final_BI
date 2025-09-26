#### Fluxo realizado pelo arquivo:
1. **Cria estrutura do DW**

   ```bash
   python create_dw.py
   ```

2. **Cargas merge e manuais**

   ```bash
   python carga_merge_ocupacao.py dados/ocupacoes.csv      # Códigos da Classificação Brasileira de Ocupações
   python carga_merge_cids.py dados/cids.json              # CID10 (Classificação Internacional de Doenças)
   python carga_merge_municipios.py dados/municipios.csv   # Dados de municípios do IBGE
   python carga_manual_circobito.py
   python carga_manual_escfal.py
   python carga_manual_estciv.py
   python carga_manual_racacor.py
   python carga_manual_sexo.py
   python carga_manual_tplocor.py
   ```
    *Cargas merge têm como fonte arquivos com os dados necessários, já as manuais foram criadas com os valores disponíveis no Dicionário de Dados do SIM (Sistema de Informação sobre Mortalidade)*

4. **Carga incremental dos óbitos**

   ```bash
   python carga_incremental_obito.py dados/obitos.csv      # Datasets de cada ano disponíveis no SIM (Sistema de Informação sobre Mortalidade)
   ```
---
