# Data Persona: O Ecossistema de Dados da ANS

> **Documento de Referência Técnica**
> Este documento detalha a natureza dos dados da ANS (Agência Nacional de Saúde Suplementar), diagnostica anomalias identificadas, define as especificidades dos arquivos e detalha a estratégia de engenharia de dados adotada para normalização, limpeza e enriquecimento.

---

## 1. O Protagonista: Demonstrações Contábeis

Estes arquivos representam o balanço financeiro das operadoras de saúde. Eles constituem o alvo principal da extração e contêm os valores monetários trimestrais que devem ser analisados.

- **Fonte Oficial:** `https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/`
- **Responsável:** Diretoria de Normas e Habilitação das Operadoras (DIOPE).
- **Granularidade:** Trimestral (acumulado por operadora).
- **Chave Primária:** `REG_ANS` (Registro na ANS).

### Defeito Crítico: Anonimato Fiscal

Este conjunto de dados possui uma característica crítica: ele contém o código da operadora (`REG_ANS`), mas **não possui** `CNPJ` nem `Razão Social`. Para gerar um relatório legalmente válido ou útil para cruzamentos fiscais, é obrigatório enriquecer estes dados com fontes cadastrais externas.

### Esquema de Colunas (Schema)

| Nome do Campo         | Tipo      | Descrição                                                                                       |
| --------------------- | --------- | ----------------------------------------------------------------------------------------------- |
| **DATA**              | Data      | Data de referência do trimestre (Formato `AAAA-MM-DD`). Geralmente o primeiro dia do trimestre. |
| **REG_ANS**           | Texto/Num | Código único da operadora. Chave de junção.                                                     |
| **CD_CONTA_CONTABIL** | Texto     | Código hierárquico da conta (Ex: `4`, `41`, `411`).                                             |
| **DESCRICAO**         | Texto     | Nome da conta contábil (Ex: `DESPESAS ASSISTENCIAIS`).                                          |
| **VL_SALDO_INICIAL**  | Decimal   | Saldo no início do período. (Pode não existir em arquivos antigos).                             |
| **VL_SALDO_FINAL**    | Decimal   | Saldo acumulado no fim do período. O valor principal da análise.                                |

### Diagnóstico de Anomalias e Variações

O código de extração e processamento foi projetado para ser resiliente às seguintes variações históricas identificadas nos arquivos da ANS:

#### A. Nomenclatura Não Padronizada dos Arquivos

Não existe padrão ISO consistente nos arquivos ZIP disponibilizados no FTP. Exemplos reais mapeados:

- **Padrão Moderno:** `1T2025.zip`, `2T2025.zip`
- **Padrão Verboso:** `2013-1t.zip`, `3-Trimestre.zip`
- **Padrão Datado:** `20130416_1T2012.zip`
- **Padrão Extenso:** `20120614_2011_1_trimestre.zip`

> **Solução Técnica:** Não confiar em divisões de string simples (split). Utilizar Expressões Regulares (Regex) para capturar o ano (`\d{4}`) e o trimestre (`\d` seguido de `t` ou `trim`), ignorando o restante do nome do arquivo.

#### B. Encoding e Codificação de Caracteres

Embora a documentação recente sugira padronização, arquivos antigos podem estar codificados em `Latin-1` (CP1252) ou conter caracteres corrompidos (ex: `DepÃ³sitos`), enquanto novos arquivos estão em `UTF-8`.

> **Solução Técnica:** Pipeline de leitura com fallback automático. O sistema tenta ler como `utf-8`; em caso de `UnicodeDecodeError`, reprocessa utilizando `latin-1`.

#### C. Variação de Cabeçalhos (Headers)

Arquivos mais antigos podem suprimir a coluna `VL_SALDO_INICIAL`. O parser deve ser flexível para mapear colunas pelo nome e não pela posição, evitando erros de índice.

#### D. Hierarquia Contábil (Risco de Duplicação)

O arquivo contém tanto as contas sintéticas ("mães") quanto as analíticas ("filhas"). Somar a coluna `VL_SALDO_FINAL` sem critérios resultará em valores duplicados ou triplicados.

> **Solução Técnica:** É necessário filtrar pelo nível analítico mais baixo ou selecionar contas específicas (ex: filtrar pela descrição exata da conta desejada).

---

## 2. Módulo de Enriquecimento: Ecossistema Cadastral (CADOP)

Para resolver a ausência de CNPJ no arquivo contábil, recorremos ao CADOP (Cadastro de Operadoras).

### O Problema da Temporalidade

O arquivo contábil é um registro histórico (ex: 1º Trimestre de 2024). Se cruzarmos esses dados apenas com a lista de operadoras **Ativas** hoje, perderemos os dados das empresas que faliram, foram fundidas ou canceladas no intervalo entre a publicação do balanço e a data atual.

### Fontes de Dados para Enriquecimento

Para garantir a integridade histórica dos dados, unificamos duas fontes oficiais:

#### Fonte A: Operadoras Ativas

- **Arquivo:** `Relatorio_cadop.csv`
- **Localização FTP:** `/operadoras_de_plano_de_saude_ativas/`
- **Função:** Base primária (cobre a vasta maioria dos casos). Representa empresas operantes e regularizadas.

#### Fonte B: Operadoras Canceladas

- **Arquivo:** `Relatorio_cadop_canceladas.csv`
- **Localização FTP:** `/operadoras_de_plano_de_saude_canceladas/`
- **Função:** Fallback Histórico. Garante que operadoras extintas ainda tenham seus CNPJs identificados nos balanços passados.
- **Diferencial:** Possui colunas exclusivas como `Data_Descredenciamento` e `Motivo_do_Descredenciamento`.

### Estrutura Cadastral Unificada (Target Schema)

Após a unificação das fontes, espera-se a seguinte estrutura para o cruzamento:

| Coluna Original      | Nome Normalizado  | Descrição                                                |
| -------------------- | ----------------- | -------------------------------------------------------- |
| `REGISTRO_OPERADORA` | **REG_ANS**       | Chave Primária padronizada para Join.                    |
| `CNPJ`               | **CNPJ**          | Identificador fiscal (Texto, mantendo zeros à esquerda). |
| `Razao_Social`       | **Razao_Social**  | Nome jurídico da entidade.                               |
| `Modalidade`         | **Modalidade**    | Classificação (Ex: Medicina de Grupo, Cooperativa).      |
| `Data_Registro_ANS`  | **Data_Registro** | Data de entrada no sistema.                              |

### Domínio de Dados: Região de Comercialização

Código numérico presente no CADOP que indica a abrangência geográfica da operadora:

- **1:** Nacional.
- **2:** Grupo de Estados (incluindo SP).
- **3:** Estadual (Único estado, exceto SP).
- **4:** Municipal (Capitais específicas).
- **5:** Grupo de Municípios.
- **6:** Municipal (Outros).

---

## 3. Estratégia de Acesso e Scraping

A navegação no FTP da ANS exige manipulação de URL e interpretação de HTML.

### Parâmetros de Ordenação da URL

Para garantir a obtenção dos arquivos corretos via script, utilizam-se parâmetros de query string no servidor Apache da ANS:

- `?C=N;O=D`: Ordenar por **N**ome (Decrescente).
- `?C=M;O=A`: Ordenar por Data de **M**odificação (Crescente) - _Útil para identificar a versão mais recente_.
- `?C=S;O=A`: Ordenar por Tamanho (**S**ize).

### Estrutura de Diretórios

- **Raiz:** `YYYY/` (Ex: `2025/`, `2024/`).
- **Conteúdo:** Arquivos `.zip` (preferenciais) ou `.csv`.
- **Risco Mapeado:** Alguns trimestres podem conter múltiplos arquivos devido a republicações. A estratégia deve priorizar o arquivo com data de modificação mais recente.

---

## 4. Pipeline de Engenharia de Dados (ETL)

Para assegurar a consistência e qualidade dos dados finais, o pipeline segue estritamente os passos abaixo:

### Passo 1: Extração (Scraper)

1. Iterar sobre os diretórios de `Demonstracoes_Contabeis`.
2. Baixar os arquivos ZIP aplicando o filtro de trimestres solicitados.
3. Baixar obrigatoriamente `Relatorio_cadop.csv` (Ativas) e `Relatorio_cadop_canceladas.csv` (Canceladas).

### Passo 2: Transformação e Unificação (Processor)

A lógica de processamento deve tratar a mudança de schema (Schema Drift) e priorização de dados:

1. **Normalização:** Renomear a coluna `REGISTRO_OPERADORA` para `REG_ANS` em ambos os arquivos do CADOP para permitir a junção. Garantir que a coluna `CNPJ` seja tratada como string (texto) para não perder zeros à esquerda.
2. **Empilhamento (Stacking):** Concatenar verticalmente o DataFrame de Ativas e o DataFrame de Canceladas, criando um _DataFrame Mestre de Operadoras_.
3. **Deduplicação Inteligente:**

- Ordenar o DataFrame Mestre por `Data_Registro_ANS` (ou data de atualização do arquivo) de forma decrescente.
- Remover duplicatas baseadas na chave `REG_ANS`, mantendo a ocorrência mais recente (`keep='first'`). Isso prioriza os dados da tabela de Ativas caso a operadora conste em ambas por erro sistêmico ou transição recente.

### Passo 3: Enriquecimento (Join)

A junção final entre os dados financeiros e cadastrais segue a lógica de Left Merge:

```text
Resultado = Tabela_Contabil (Left Join) Tabela_Mestra_Operadoras ON "REG_ANS"

```

- **Justificativa:** O uso do Left Join garante que **nenhum dado financeiro seja descartado**, mesmo que a operadora não seja encontrada no cadastro (situação de borda).
- **Tratamento de Falhas:** Se o campo `CNPJ` resultar em nulo após o join, preencher com "NAO_ENCONTRADO" para fins de auditoria e logs de qualidade de dados.

### Passo 4: Limpeza, Filtros e Consolidação

- **Conversão de Tipos:** Converter colunas de data para formato ISO 8601 e garantir que valores monetários sejam `float`.
- **Filtro de Contas:** Filtrar especificamente a conta contábil solicitada ("EVENTOS/ SINISTROS...") seja pelo código da conta ou pela descrição textual.
- **Consolidação (Agrupamento):** O dado bruto possui granularidade analítica (subcontas). Aplicamos uma agregação (`SUM`) agrupando por `CNPJ`, `Ano` e `Trimestre` para gerar uma visão gerencial unificada (uma linha por empresa), eliminando duplicatas visuais e atendendo ao requisito do relatório final.

---

## 5. Definições Técnicas de Parsing (CSV)

Parâmetros mandatórios para a leitura correta dos arquivos brutos da ANS (via Pandas ou bibliotecas similares):

- **Delimitador:** Ponto e vírgula (`;`).
- **Caractere de Citação (Quotechar):** Aspas duplas (`"`). Essencial para campos de texto que contêm delimitadores internos (ex: logradouros ou razões sociais complexas).
- **Separador Decimal:** Vírgula (`,`). Deve ser convertido para ponto flutuante (float).
- **Tratamento de Encoding:** Tentativa primária em `utf-8`; fallback secundário para `latin-1` (cp1252) em caso de falha.
