# Teste Técnico - Intuitive Care

Solução desenvolvida para o desafio de Engenharia de Dados e Integração (Backend), focada na extração, normalização e consolidação de dados financeiros da ANS (Agência Nacional de Saúde Suplementar).

> 📘 **Documentação de Referência (Data Persona)**
> Este projeto acompanha um documento técnico detalhado (`DATA_PERSONA.md`) que descreve o esquema dos dados, o dicionário de variáveis e o mapeamento profundo das anomalias históricas da ANS.
>
> **[Clique aqui para acessar a Documentação Completa de Dados](./DATA_PERSONA.md)**

## 🚀 Visão Geral e Arquitetura

O projeto adota uma arquitetura de **Pipeline ETL Desacoplado**, priorizando a rastreabilidade dos dados e a resiliência contra inconsistências comuns em fontes governamentais.

### O Pipeline

1. **Extract (Scraper):** Crawler que mapeia o FTP da ANS, identifica a estrutura de diretórios e baixa os arquivos mais recentes (Contábeis + Cadastrais), lidando automaticamente com a virada de ano.
2. **Transform (Processor):** Normalização de _encodings_ (UTF-8/Latin-1), unificação de formatos (CSV/XLSX) e limpeza de dados.
3. **Enrich & Load (Consolidação):** Enriquecimento cadastral (Join com CADOP), agregação de valores e geração do relatório final compactado.

---

## 🛠️ Decisões Técnicas e Trade-offs

Esta seção documenta as escolhas de engenharia, justificando o caminho adotado em detrimento de outras possibilidades, focando em praticidade e eficiência (KISS).

### 1. Orquestração (`main.py`)

- **Execução Sequencial (Batch) vs. Streaming**
  - **Decisão:** O pipeline baixa todos os arquivos necessários antes de iniciar o processamento.
  - **Justificativa:** Para o volume de dados proposto (3 trimestres), a complexidade de uma arquitetura _Producer-Consumer_ (Async) não se justifica. O modelo sequencial facilita o tratamento de erros e garante que processamos apenas se o download for bem-sucedido.
- **Handover via Sistema de Arquivos**
  - **Decisão:** A troca de dados entre Scraper e Processor ocorre via persistência na pasta `data/raw`, e não em memória.
  - **Justificativa:** Garante **auditabilidade**. Caso o processamento falhe (bug de parsing), os dados brutos já estão salvos, permitindo reexecutar a transformação sem onerar o servidor da ANS com novos downloads.

### 2. Extração de Dados (`scraper.py`)

- **Requests (HTTP Leve) vs. Selenium (Browser)**
  - **Decisão:** Utilização de `requests` + `BeautifulSoup` para navegação no diretório do Apache.
  - **Justificativa:** O servidor da ANS é estático. Usar automação de browser seria _overengineering_, consumindo muito mais memória e tempo. A solução via HTTP é ordens de magnitude mais rápida.
- **Identificação Heurística (Regex)**
  - **Decisão:** Uso de Expressões Regulares (`[1-4].*(t|trim)`) para identificar arquivos.
  - **Justificativa:** Atende ao requisito de resiliência. A ANS não possui padrão estrito de nomenclatura (ex: `1T2025.zip` vs `2024_1_trim.zip`). Regex garante a captura independente do formato humano utilizado na nomeação.

### 3. Processamento e ETL (`processor.py`)

- **Extração em Disco vs. In-Memory Streams**
  - **Decisão:** Extração física dos arquivos ZIP para diretório temporário antes da leitura.
  - **Justificativa:** Arquivos legados frequentemente misturam encodings. Ler do disco permite que a engine C do Pandas detecte e trate melhor falhas de codificação do que streams de bytes puros.
- **Estratégia de Fallback de Encoding**
  - **Decisão:** Tentativa hierárquica: primeiro `UTF-8` (padrão moderno), depois `Latin-1` (legado).
  - **Justificativa:** Maximiza a taxa de sucesso na leitura de arquivos de décadas diferentes sem intervenção manual, evitando erros de _Mojibake_ (caracteres corrompidos).
- **Consolidação por Agrupamento (GroupBy)**
  - **Decisão:** Agregação total dos valores por CNPJ e Trimestre.
  - **Justificativa:** O dado bruto é contábil e detalhado (subcontas). Para atender ao requisito de "Consolidação", sacrificamos a granularidade analítica em favor de uma visão gerencial unificada (uma linha por empresa), eliminando duplicatas visuais.

---

## 🕵️ Análise de Inconsistências e Tratamento de Dados

Conforme solicitado no requisito 1.3, abaixo está a matriz de inconsistências identificadas nos dados brutos e a estratégia adotada para mitigação:

| Inconsistência Encontrada  | Ação Técnica                  | Justificativa (Pensamento Crítico)                                                                                                                                                               |
| :------------------------- | :---------------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **CNPJs Duplicados**       | **Agrupamento (Sum)**         | O arquivo original detalha subcontas contábeis ("mães" e "filhas"). Agrupamos por `CNPJ` + `Trimestre` somando os valores para evitar duplicidade de linhas no relatório final.                  |
| **Ausência de CNPJ**       | **Enrichment (CADOP)**        | O arquivo financeiro possui apenas o registro `REG_ANS`. Cruzamos (Left Join) com a base do CADOP para enriquecer com o CNPJ e Razão Social oficiais.                                            |
| **Operadoras "Fantasmas"** | **CADOP Ativas + Canceladas** | Operadoras que faliram ainda possuem histórico contábil. Cruzamos também com a base de "Canceladas" para garantir que nenhum dado financeiro histórico seja perdido por falta de cadastro atual. |
| **Valores Negativos**      | **Mantidos**                  | Contabilmente, despesas negativas representam reversões de provisão ou glosas. Zerá-las distorceria o saldo real da operadora (Audit Trail).                                                     |
| **Valores Zerados**        | **Removidos**                 | Linhas com valor estritamente `0.0` foram descartadas para otimizar o tamanho do arquivo, pois não representam movimentação financeira.                                                          |
| **Datas Inconsistentes**   | **Inferência via Nome**       | A coluna "DATA" interna dos arquivos é frequentemente genérica (ex: 01/01). Derivamos o `Ano` e `Trimestre` diretamente do nome do arquivo ZIP, que provou ser a fonte mais confiável.           |
| **Formatos Heterogêneos**  | **Detecção Automática**       | O sistema identifica automaticamente se o arquivo é `.csv`, `.txt` ou `.xlsx` e aplica o loader correto do Pandas.                                                                               |

---

## 🛡️ Parte 2: Transformação, Validação e Agregação

Esta seção detalha as estratégias de Engenharia de Dados aplicadas para garantir a qualidade e enriquecimento do dataset, conforme requisitos 2.1, 2.2 e 2.3.

### 2.1 Qualidade e Validação de Dados (`validator.py`)

Implementação de um motor de regras para auditoria dos dados consolidados.

- **Regras Implementadas:**
  1. **CNPJ:** Validação matemática de dígitos verificadores (Módulo 11).
  2. **Razão Social:** Verificação de existência e nulidade.
  3. **Valores:** Detecção de valores negativos (inconsistência potencial em despesas).

#### ⚖️ Trade-off: Soft Validation (Flagging) vs. Hard Validation (Drop)

- **Decisão:** Adotar estratégia de **Flagging**. Registros inválidos são mantidos no dataset final, mas marcados com colunas booleanas (`Registro_Conforme`, `CNPJ_Valido`).

- **Justificativa:**
  - _Auditabilidade:_ Permite que analistas rastreiem a origem do erro (falha na fonte da ANS vs. erro de ETL).
  - _Integridade Financeira:_ Em contabilidade, valores negativos podem ser reversões legítimas. Excluí-los silenciosamente distorceria o balanço final do setor.
  - _Transparência:_ O consumidor do dado recebe a informação completa e decide se filtra (`WHERE Registro_Conforme = True`) ou se investiga as anomalias.

---

### 2.2 Enriquecimento e Tratamento de Falhas (Join)

Cruzamento das Demonstrações Contábeis com o Mestre de Operadoras (Ativas + Canceladas) para obter `CNPJ`, `Modalidade` e `UF`.

#### 🧩 Estratégia de Join e Chaves

- **Chave de Ligação:** Utilizamos `RegistroANS` (REG_ANS).
  - _Motivo:_ Os arquivos contábeis brutos da ANS **não possuem CNPJ**, apenas o código `REG_ANS`. O join é técnico (código-para-código) para então recuperar o CNPJ fiscal.

- **Tipo de Join:** `Left Join` (Contábil → Cadastral).

#### ⚖️ Trade-off: Integridade Financeira vs. Cadastral

Como tratar registros contábeis que não possuem correspondência no arquivo de cadastro ("Orfãos")?

- **Decisão:** **Preservação com Fallback**.
  - Preenchemos dados faltantes com placeholders: `CNPJ="NAO_ENCONTRADO"`, `RazaoSocial="OPERADORA_NAO_IDENTIFICADA"`.
  - Utilizamos `dropna=False` na agregação final.
- **Justificativa:** A prioridade é o **Saldo Financeiro**. Se uma operadora movimentou valores (tem balanço), esse dinheiro deve constar no relatório total, mesmo que o cadastro da empresa esteja falho na fonte. O registro é salvo, mas marcado como `Registro_Conforme=False` pelo validador.

---

### 2.3 Agregação e Estatística (`despesas_agregadas.csv`)

Geração de visão analítica agrupada por Operadora e Estado (UF).

#### 📊 Métricas Calculadas

1. **Valor Total:** Soma do período (KPI principal).
2. **Média Trimestral:** Ticket médio de despesa.
3. **Desvio Padrão:** Medida de volatilidade/risco.
   - _Tratamento:_ Operadoras com apenas 1 trimestre recebem desvio `0.0` (sem variação).

#### ⚖️ Trade-off: Ordenação

- **Decisão:** Ordenação em memória (`QuickSort`) por Valor Total Decrescente.

- **Justificativa:**
  - _Performance:_ O dataset agregado (uma linha por empresa) é pequeno o suficiente para caber na RAM, tornando desnecessário o uso de ordenação externa (disco) ou banco de dados.
  - _Negócio:_ A ordenação decrescente favorece a análise de "Curva ABC", destacando imediatamente as operadoras com maior impacto sistêmico.

---

## 🗄️ Parte 3: Modelagem de Dados e SQL

Conforme solicitado na Tarefa 3, foi desenvolvida a modelagem de banco de dados e queries analíticas para explorar o dataset processado.

**📄 Arquivo de Entrega:** `queries_analiticas.sql` (na raiz do projeto).

### 3.1 Modelagem (Star Schema)

Optou-se pela **Normalização (Opção B)**, separando os dados em duas tabelas principais:

1. **Dimensão (`operadoras`):** Contém dados cadastrais mutáveis (Razão Social, UF).
2. **Fato (`despesas_consolidadas`):** Contém os eventos financeiros imutáveis, referenciando a dimensão via chave estrangeira.

**Justificativa:** Essa abordagem economiza armazenamento e otimiza a performance de agregações (SUM/AVG), pois a engine do banco não precisa ler strings longas repetidas a cada trimestre.

### 3.2 Decisões de Tipagem

- **Valores Monetários:** `DECIMAL(15,2)` ao invés de `FLOAT` para garantir precisão contábil e evitar erros de ponto flutuante.
- **Datas:** Colunas inteiras separadas (`ano`, `trimestre`) para facilitar a indexação e queries de agrupamento temporal.

### 3.3 Estratégia de Deploy (Web)

> **Nota de Arquitetura:** Para a execução da **Tarefa 4 (Interface Web)**, optou-se estrategicamente por **utilizar os arquivos CSV processados** como fonte de dados, em vez de manter uma instância de banco de dados ativa.
>
> Essa decisão reduz a complexidade de infraestrutura ("Serverless") e elimina custos de cloud para este MVP, embora o código SQL fornecido comprove a capacidade de migração para um ambiente produtivo baseado em PostgreSQL/MySQL.

---

## 📂 Estrutura do Projeto

```text
/
├── .vscode/             # Configurações de ambiente (DX e Padronização)
├── data/                # Armazenamento local (ignorado no git)
│   ├── raw/             # Arquivos ZIP e CSV baixados da ANS
│   └── processed/       # Arquivo final: consolidado_despesas.zip
├── src/                 # Código Fonte
│   ├── __init__.py      # Exposição de módulos
│   ├── scraper.py       # Crawler: Download e identificação de trimestres
│   ├── processor.py     # ETL: Limpeza, Normalização e Consolidação
│   └── validator.py     # Motor de regras de qualidade de dados
├── main.py              # Orquestrador (Entrypoint)
├── sql/                 # Armazenamento de queries
│   └── queries.sql      # Script SQL da Tarefa 3
├── DATA_PERSONA.md      # Documentação Técnica de Domínio
├── requirements.txt     # Dependências do projeto
└── README.md            # Documentação Geral

```

## 🛠️ Como Executar

O projeto foi otimizado para **VS Code**, mas pode ser executado via terminal padrão.

### Pré-requisitos

- Python 3.8 ou superior
- Git

### Instalação

1. **Clone o repositório:**

```bash
git clone https://github.com/JGustavoCN/intuitive-care-challenge.git
cd intuitive-care-challenge

```

1. **Crie o Ambiente Virtual:**

```bash
# Windows
python -m venv venv
.\venv\Scripts\activate

# Linux/Mac
python3 -m venv venv
source venv/bin/activate

```

1. **Instale as Dependências:**

```bash
pip install -r requirements.txt

```

### Execução

Basta rodar o arquivo principal. O script cuidará de todo o fluxo (Download -> Processamento -> Compactação).

```bash
python main.py

```

### Resultados

Após a execução, verifique a pasta `data/`:

- Os arquivos brutos estarão em `data/raw`.
- O arquivo final solicitado estará em: **`data/processed/consolidado_despesas.zip`**

---

_Desenvolvido como parte do processo seletivo da Intuitive Care._
