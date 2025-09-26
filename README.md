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
* Arquivos de origem (CIDs em JSON, municípios/ocupações em CSV, óbitos em CSV).
* SQLite para persistência do DW (`DW.db`).

### ▶️ Execução


1. **Clone o repositório**
  ```bash
   git clone https://github.com/diogopaz/Projeto_Final_BI.git
   ```
2. **Baixe os arquivos necessários para a carga**
   - Classificação Brasileira de Ocupações: https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/cbo/servicos/downloads/cbo2002-ocupacao.csv
   - API com os códigos e descrições do CID10 (Classificação Internacional de Doenças): https://cid10.cpp-ti.com.br/api _(baixar a página em .json)_
   - Arquivos _(.csv)_ com os dados de mortalidade de 2019 a 2024 do SIM (Sistema de Informação sobre Mortalidade): https://dados.gov.br/dados/conjuntos-dados/sim-1979-2019
   - Arquivos _(estimativa_dou_20XX.xls)_ de estimativa de população de 2019 a 2024 (2022 e 2023 não estão disponíveis) do IBGE: https://www.ibge.gov.br/estatisticas/sociais/populacao/9103-estimativas-de-populacao.html?=&t=downloads
3. ****


Para realizar a carga, basta executar o seguinte comando:
  ```bash
   python carga_final.py
   ```
*O arquivo carga_final.py executa todas as cargas na ordem correta*
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

## 📂 Estrutura do Repositório

```
├── create_dw.py               # Criação do DW
├── carga_merge_cids.py        # Carga merge da dimensão CID
├── carga_merge_ocupacao.py    # Carga merge da dimensão Ocupação
├── carga_merge_municipios.py  # Carga merge da dimensão Município
├── carga_incremental_obito.py # Carga incremental dos óbitos
├── carga_final.py             # Carga completa
├── carga_manual_*.py          # Scripts auxiliares de carga manual
├── DW.db                      # Banco de dados SQLite (gerado)
├── dados/                     # Arquivos de origem
└── README.md                  # Documentação do projeto
```

---
