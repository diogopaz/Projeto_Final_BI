#### Fluxo realizado pelo script carga_full_dm:

1.  **Criação da estrutura do DM**

    ``` bash
    python cargas_dm/create_dm.py
    ```

2.  **Cargas das dimensões**

    ``` bash
    python cargas_dm/carga_dime_tempo_ano.py   # Dimensão Tempo (Ano)
    python cargas_dm/carga_dime_circobito.py   # Dimensão Circunstância do Óbito
    python cargas_dm/carga_dime_tplocor.py     # Dimensão Tipo do Local de Ocorrência
    python cargas_dm/carga_dime_local.py       # Dimensão Local (Município, Estado, Região)
    python cargas_dm/carga_dime_pessoa.py      # Dimensão Pessoa (sexo, idade, raça/cor, etc.)
    python cargas_dm/carga_dime_causa.py       # Dimensão Causa (CID básica e terminal)
    python cargas_dm/carga_dime_tempo_dia.py   # Dimensão Tempo (Dia)
    ```

3.  **Cargas das tabelas fato**

    ``` bash
    python cargas_dm/carga_fato_populacao.py   # Fato População
    python cargas_dm/carga_fato_obito.py       # Fato Óbito
    ```

------------------------------------------------------------------------

Esse fluxo garante que:
- O **DM é criado** com a estrutura dimensional adequada e suas tabelas são truncadas para garantir a carga full.
- As **dimensões** são preenchidas com dados de tempo, pessoa, causas e
local.
- As **tabelas fato** armazenam as métricas de população e óbitos,
permitindo análises OLAP detalhadas.
