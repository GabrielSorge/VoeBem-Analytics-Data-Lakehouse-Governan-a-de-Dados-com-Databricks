# VoeBem Analytics — Data Lakehouse & Governança de Dados com Databricks

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=Databricks&logoColor=white)](https://databricks.com/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-000000?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)

---

## 📌 Sobre o Projeto

O **VoeBem Analytics** é um projeto de Engenharia de Dados de alta performance desenvolvido durante a **Imersão de Dados com IA da Alura**. O objetivo central é a construção de um **Data Lakehouse governado e escalável** utilizando o ecossistema Databricks para processar dados abertos da **ANAC (Agência Nacional de Aviação Civil)**.

A solução consolida 12 meses do histórico de **Voos Regulares Ativos (VRA)**, companhias aéreas e aeródromos brasileiros, transformando dados brutos em ativos confiáveis, totalmente documentados e otimizados para consumo por **Modelos de IA (LLMs)** e ferramentas de **Business Intelligence**.

---

##  Arquitetura Lakehouse (Arquitetura Medalhão)

A solução adota o padrão **Medallion Architecture**, dividindo o ciclo de vida dos dados em camadas de maturidade gerenciadas pelo **Unity Catalog**:

┌────────────────┐     ┌────────────────┐     ┌────────────────┐
│  Raw Storage   │ ──> │ Bronze Schema  │ ──> │ Silver Schema  │ ──> Gold Schema
│  (Volumes CSV) │     │ (Ingestão Raw) │     │ (Limpeza/Qual) │     (KPIs/Consumo)
└────────────────┘     └────────────────┘     └────────────────┘


###  Organização no Unity Catalog
* **Catálogo:** `voebem`
* **Volume (Landing Zone):** `voebem.bronze.v_raw_anac` (Armazenamento dos CSVs brutos)
* **Esquemas (Schemas):** `bronze`, `silver`, `gold`

---

##  Mapeamento do Pipeline de Dados

###  1. Camada Bronze — *Raw Ingestion*
* **Objetivo:** Ingestão bruta, imutável e idempotente dos dados originais sem perda de informação.
* **Características & Boas Práticas:**
  * **Zero Filtros:** Carga integral dos arquivos CSV preservando 100% da estrutura de origem.
  * **Tipagem Conservadora:** Armazenamento inicial de todas as colunas como `STRING` para evitar descarte prematuro por falha de schema.
  * **Metadados de Ingestão:** Adição de campos de auditoria técnica:
    * `_arquivo_origem`: Rastreabilidade do arquivo CSV processado (via `_metadata.file_name`).
    * `_ingerido_em`: Timestamp exato do momento da ingestão.
* **Volume Processado:** **1.014.705 registros** unificados na tabela `voebem.bronze.vra`.

---

###  2. Camada Silver — *Governed Mirror* & Qualidade
* **Objetivo:** Limpeza técnica, padronização de tipos, tratamento de valores nulos e cálculo de métricas nativas sem violar regras de negócio.
* **Princípio Pétreo:** *A Silver é o espelho governado da Bronze*. Não descarta dados nem aplica decisões comerciais restritivas (ex: filtros de datas ou eliminação de registros atípicos).
* **Tratamentos & Engenharia de Dados Aplicada:**
  1. **Tipagem Estrita e Segura:**
     * Utilização de `try_cast(... AS TIMESTAMP)` para tratar múltiplos formatos de data/hora de partida e chegada real/prevista.
     * Tipagem numérica para códigos ICAO/IATA e contadores de passageiros/carga.
  2. **Tratamento de Indefinições:**
     * Aplicação de `NULLIF(coluna, 'null')` para converter strings `'null'` e vazias em valores nulos nativos de banco de dados (`NULL`).
  3. **Aritmética Direta de Métricas:**
     * Cálculo exato dos atrasos em minutos:
       * `atraso_partida_min = TIMESTAMPDIFF(MINUTE, partida_prevista, partida_real)`
       * `atraso_chegada_min = TIMESTAMPDIFF(MINUTE, chegada_prevista, chegada_real)`
       * `minutos_recuperados = atraso_partida_min - atraso_chegada_min`
  4. **Unificação de Domínios:**
     * Consolidação dos cadastros de empresas aéreas nacionais e internacionais em uma visão padronizada (`voebem.silver.empresas`).
  5. **Metadados de Pipeline & Auditoria:**
     * Adição dos campos: `data_carga`, `usuario_executor` e `versao_pipeline`.

---

##  Governança de Dados & Preparação para IA (Databricks Genie)

O Data Lakehouse foi otimizado para interagir com soluções de **IA Generativa (LLMs)** e com o **Databricks Genie** (IA Conversacional do Databricks):

1. **Documentação de Campo (100% Coverage):**
   * Todas as colunas de todas as tabelas na camada Silver possuem comentários nativos em linguagem de negócio via `COMMENT ON COLUMN`.
2. **Tagging de Ativos:**
   * Aplicação de tags no nível de tabela e catálogo para facilitar a busca e contexto semântico do Agentic AI (`camada`, `dominio`, `fonte`, `grao`).
3. **Catálogo de Serviços Silver Criados:**
   * `voebem.silver.vra`: Histórico completo de etapas de voo tratadas.
   * `voebem.silver.empresas`: Cadastro unificado de operadores aéreos.
   * `voebem.silver.aerodromos`: Cadastro oficial de aeródromos públicos brasileiros.
   * `voebem.silver.codigos_operacao`: Tabela de referência e domínio dos códigos da ANAC.

---

##  Orquestração & Rastreabilidade (Databricks Jobs)

O pipeline é totalmente orquestrado através do **Databricks Jobs**, permitindo:
* Execução em esteira automatizada com dependência DAG (Bronze ➔ Silver ➔ Gold).
* Rastreabilidade fim a fim (Data Lineage) visível nativamente no **Unity Catalog**.
* Reprocessamento idempotente em caso de falhas ou reexecução programada.

---

##  Testes de Qualidade & SQL Snippets

### 1. Teste de Paridade de Linhas (Bronze vs. Silver)
Garantia de que a camada Silver manteve a integridade completa dos dados sem perdas não documentadas:

```sql
SELECT 
    (SELECT COUNT(*) FROM voebem.bronze.vra) AS linhas_bronze,
    (SELECT COUNT(*) FROM voebem.silver.vra) AS linhas_silver,
    (SELECT COUNT(*) FROM voebem.bronze.vra) - (SELECT COUNT(*) FROM voebem.silver.vra) AS diferenca_abs;
```
-- Resultado esperado: 1.014.705 | 1.014.705 | Diferença: 0

### 2. Validação Automática de Cobertura de Governança
Query de auditoria no information_schema para assegurar que 100% das colunas possuem documentação:

```SQL
SELECT 
    table_name,
    COUNT(*) AS total_colunas,
    SUM(CASE WHEN comment IS NOT NULL AND comment <> '' THEN 1 ELSE 0 END) AS colunas_documentadas,
    ROUND(100.0 * SUM(CASE WHEN comment IS NOT NULL AND comment <> '' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_cobertura
FROM voebem.information_schema.columns
WHERE table_schema = 'silver'
GROUP BY table_name;
```
## Tecnologias Utilizadas
* Computação e Plataforma: Databricks / Unity Catalog
* Motor de Dados: Apache Spark (PySpark & Databricks SQL Engine)
* Formato de Armazenamento: Delta Lake
* Inteligência Artificial: Databricks Assistant & Databricks Genie
* Orquestração: Databricks Jobs
* Linguagens: SQL & Python

## Autor
Gabriel Sorge de Almeida
Graduando em Ciência de Dados e Inteligência Artificial na PUC-Campinas
Foco de Atuação: Engenharia de Dados, Cloud Computing (AWS/Databricks), Pipeline de Dados & IA.
