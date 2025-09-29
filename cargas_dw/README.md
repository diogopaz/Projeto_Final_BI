## Fluxo realizado pelo script carga_dw:

1.  **Criação da estrutura do DW**

    ``` bash
    python cargas_dw/create_dw.py
    ```

2.  **Cargas manuais das dimensões**\
    (valores fixos definidos a partir do Dicionário de Dados do SIM)

    ``` bash
    python cargas_dw/carga_manual_circobito.py    # Circunstância do Óbito
    python cargas_dw/carga_manual_escfal.py       # Escolaridade
    python cargas_dw/carga_manual_estcivil.py     # Estado Civil
    python cargas_dw/carga_manual_racacor.py      # Raça/Cor
    python cargas_dw/carga_manual_sexo.py         # Sexo
    python cargas_dw/carga_manual_tplocor.py      # Tipo do Local de Ocorrência
    ```

3.  **Cargas merge de dimensões**\
    (integração com arquivos externos: CBO, CID10 e IBGE)

    ``` bash
    python cargas_dw/carga_merge_cids.py ./cid.json                   # CID10 (Classificação Internacional de Doenças)
    python cargas_dw/carga_merge_municipios.py ./Municipios_IBGE.csv  # Municípios do IBGE
    python cargas_dw/carga_merge_ocupacao.py ./cbo2002_ocupacao.csv   # Códigos da Classificação Brasileira de Ocupações
    ```
    
4. **Carga incremental das populações**
    ``` bash
    python cargas_dw/carga_incremental_populacao.py cargas_dw/estimativa_dou_2018.xls
    python cargas_dw/carga_incremental_populacao.py cargas_dw/estimativa_dou_2019.xls
    python cargas_dw/carga_incremental_populacao.py cargas_dw/estimativa_dou_2020.xls
    python cargas_dw/carga_incremental_populacao.py cargas_dw/estimativa_dou_2021.xls
    python cargas_dw/carga_incremental_populacao.py cargas_dw/estimativa_dou_2022.xls
    python cargas_dw/carga_incremental_populacao.py cargas_dw/estimativa_dou_2024.xls
    ```


5.  **Carga incremental dos óbitos (fato principal)**\
    (datasets anuais do SIM - Sistema de Informação sobre Mortalidade)

    ``` bash
    python cargas_dw/carga_incremental_obito.py cargas_dw/Mortalidade_Geral_2018.csv
    python cargas_dw/carga_incremental_obito.py cargas_dw/Mortalidade_Geral_2019.csv
    python cargas_dw/carga_incremental_obito.py cargas_dw/Mortalidade_Geral_2020.csv
    python cargas_dw/carga_incremental_obito.py cargas_dw/Mortalidade_Geral_2021.csv
    python cargas_dw/carga_incremental_obito.py cargas_dw/Mortalidade_Geral_2022.csv
    python cargas_dw/carga_incremental_obito.py cargas_dw/Mortalidade_Geral_2023.csv
    python cargas_dw/carga_incremental_obito.py cargas_dw/Mortalidade_Geral_2024.csv
    ```

------------------------------------------------------------------------

Esse fluxo garante que:
- O **DW é criado** com a estrutura correta.
- As **dimensões manuais** são preenchidas com os valores fixos.
- As **dimensões externas** são carregadas via merge com dados oficiais
(CID, IBGE, CBO).
- Finalmente, os **óbitos** de cada ano são carregados na tabela fato,
permitindo análises históricas.
