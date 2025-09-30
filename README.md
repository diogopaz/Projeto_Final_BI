# Projeto Final BI – Mortalidade por Causas Externas

## 📖 Introdução

Este projeto foi desenvolvido no contexto do **Programa de Estágio em TI – 2025 (Turma de BI)**.
O tema explorado foi **Mortalidade por Causas Externas**, com o objetivo de **investigar como fatores sociais e territoriais influenciam a mortalidade por causas não naturais**, destacando áreas e grupos mais afetados.

Para isso, construímos um **Data Warehouse (DW)** em SQLite e uma **camada multidimensional (DM)** que alimenta dashboards no **Tableau**, possibilitando análises sobre:

* Distribuição regional da mortalidade por causas externas.
* Grupos populacionais mais afetados (sexo, idade, ocupação).
* Correlação com fatores sociais e territoriais.

---

## 🔄 Fluxo de ETL

### 1. Ingestão de dados (DW)

* Os dados de mortalidade (SIM), municípios (IBGE), CIDs e ocupações foram coletados em formato **CSV/JSON**.
* Scripts Python tratam e padronizam os dados antes de inseri-los no DW.
* Estratégias de carga utilizadas:

  * **Merge** para cadastros (CID, Ocupação, Município, Estado, Região).
  * **Incremental** para "movimentos" de óbitos.
  *  **Manual** para valores de domínio.
* Padronização de nomes segue as convenções:

  * `DWCD_` para cadastros (dimensões candidatas).
  * `DWMV_` para movimentos (fatos candidatos).
  * `SK_` para surrogate keys, `CD_` para chaves naturais, `NM_` para nomes, `DS_` para descrições, `DT_` para datas.

### 2. Camada multidimensional (DM)

* Estrutura em **esquema estrela**:

  * **Fato**: FATO\_OBITO (mortalidade por causas externas).
  * **Dimensões**: DIME\_CID, DIME\_OCUPACAO, DIME\_MUNICIPIO, DIME\_ESTADO, DIME\_REGIAO, DIME\_TEMPO.
* Carga **full** garante dados consolidados e prontos para uso no Tableau.

---

## 🚀 Como Executar

### 📋 Pré-requisitos

* Python 3.9+
* Bibliotecas:

  ```bash
  pip install pandas
  ```
* Arquivos de origem (CIDs em JSON, municípios/ocupações em CSV, óbitos em CSV, populações em XLS).
* SQLite para persistência do Data Warehouse (`DW.db`) e Data Mart (`DM.db`).

### ▶️ Execução
1. **Clone o repositório**
  ```bash
   git clone https://github.com/diogopaz/Projeto_Final_BI.git
   ```
2. **Baixe os arquivos necessários para a carga**
   - Classificação Brasileira de Ocupações: https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/cbo/servicos/downloads/cbo2002-ocupacao.csv
   - API com os códigos e descrições do CID10 (Classificação Internacional de Doenças): https://cid10.cpp-ti.com.br/api _(baixar a página em .json)_
   - Arquivos _(Mortalidade_Geral_20XX.csv)_ com os dados de mortalidade de 2019 a 2024 do SIM (Sistema de Informação sobre Mortalidade): https://dados.gov.br/dados/conjuntos-dados/sim-1979-2019
   - Arquivos _(estimativa_dou_20XX.xls)_ de estimativa de população de 2019 a 2024 (2022 e 2023 não estão disponíveis) do IBGE: https://www.ibge.gov.br/estatisticas/sociais/populacao/9103-estimativas-de-populacao.html?=&t=downloads
   - Arquivo de municípios brasileiros _(municipios.csv)_: https://github.com/kelvins/municipios-brasileiros/blob/main/csv/municipios.csv
3. **Salve os arquivos baixados na pasta cargas_dw dentro do repositório clonado**
4. **Execute o script de carga do DW**
   ```bash
   python carga_dw.py
   ```
5. **Execute o script de carga do DM**
   ```bash
   python carga_full_dm.py
   ```
## 📂 Estrutura do Repositório

```
├── cargas_dm/                     # Scripts de carga do Data Mart (DM)
│   ├── carga_dime_*.py            # Cargas das dimensões
│   ├── carga_fato_*.py            # Cargas das fatos
│   └── create_dm.py               # Criação do Data Mart
│
├── cargas_dw/                     # Scripts de carga do Data Warehouse (DW)
│   ├── carga_incremental_*.py     # Cargas incrementais (Óbitos, Estimativas de População)
│   ├── carga_merge_*.py           # Cargas merge (CIDs, Municípios e Ocupações)
│   ├── carga_manual_*.py          # Cargas manuais (Campos com valores de domínio)
│   └── create_dw.py               # Criação do Data Warehouse
│
├── README.md                      # Documentação do projeto
├── carga_dw.py                    # Script principal de carga do DW
└── carga_full_dm.py               # Script de carga full do DM

```
