# 🕵️‍♂️ Persona dos Dados: Diagnóstico e Estratégia

> **Documento de Referência Técnica**
> Este documento detalha a natureza dos dados da ANS, suas anomalias identificadas e a estratégia de engenharia adotada para normalização e enriquecimento.

---

## 1. O Protagonista: Demonstrações Contábeis

Estes arquivos representam o balanço financeiro das operadoras de saúde. Eles são o alvo principal da extração.

- **Fonte:** `https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/`
- **Granularidade:** Trimestral (acumulado por operadora).
- **Chave Primária:** `REG_ANS` (Registro na ANS). **Atenção:** Não possui CNPJ.

### 🎭 Personalidade (Variações e Anomalias)

Os dados não são padronizados ao longo do tempo. O código deve ser resiliente às seguintes "mudanças de humor":

#### A. Nomenclatura dos Arquivos (Caos Criativo)

Não existe um padrão único de nomeação nos ZIPs. Exemplos reais mapeados:

- **Padrão Moderno:** `1T2025.zip`, `2T2025.zip`
- **Padrão Verboso:** `2013-1t.zip`, `3-Trimestre.zip`
- **Padrão Datado:** `20130416_1T2012.zip`
- **Padrão Extenso:** `20120614_2011_1_trimestre.zip`

> **Solução Técnica:** Não confiar em `split()`. Utilizar **Regex** para capturar o ano (`\d{4}`) e o trimestre (`\d` seguido de `t` ou `trim`).

#### B. Cabeçalhos (Headers) Mutantes

As colunas mudam dependendo da época:

- **Layout Completo:** `"DATA";"REG_ANS";"CD_CONTA_CONTABIL";"DESCRICAO";"VL_SALDO_INICIAL";"VL_SALDO_FINAL"`
- **Layout Antigo:** `"DATA";"REG_ANS";"CD_CONTA_CONTABIL";"DESCRICAO";"VL_SALDO_FINAL"`

> **Solução Técnica:** Normalização durante a leitura. Se `VL_SALDO_INICIAL` não existir, assumir `0.0` ou ignorar se o foco for apenas o saldo final.

#### C. Encoding (A Pegadinha)

Embora se apresentem como CSVs modernos, muitos arquivos antigos (e até alguns novos) contêm caracteres como `DepÃ³sitos` ou usam codificação `Latin-1` (ANSI) em vez de `UTF-8`.

- **Estratégia:** Tentar ler como `utf-8`. Em caso de `UnicodeDecodeError`, fazer fallback para `latin-1` (cp1252).

---

## 2. O Elo Perdido: Enriquecimento Cadastral (CADOP)

Como os arquivos contábeis **não possuem CNPJ nem Razão Social**, precisamos buscar essas informações externamente.

### O Problema da Temporalidade ⏳

Se cruzarmos os dados contábeis de um trimestre passado apenas com a lista de operadoras **Ativas** hoje, perderemos informações de operadoras que faliram ou foram canceladas nesse intervalo.

### 🧠 Estratégia de Enriquecimento (Join)

Para garantir a integridade histórica, criaremos uma **Tabela Mestra de Operadoras** unificando duas fontes:

| Fonte          | URL                                                                       | Função                                                                                                    |
| :------------- | :------------------------------------------------------------------------ | :-------------------------------------------------------------------------------------------------------- |
| **Ativas**     | `/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv`                | Base primária (90%+ dos casos).                                                                           |
| **Canceladas** | `/operadoras_de_plano_de_saude_canceladas/Relatorio_cadop_canceladas.csv` | **Fallback Histórico**. Garante que operadoras extintas ainda tenham seus CNPJs identificados no passado. |

> **Nota de Decisão:** Fontes como "Operadoras Acreditadas" ou "Não Hospitalares" foram descartadas por serem subconjuntos ou fora do escopo financeiro principal.

---

## 3. Acesso e Filtros (Scraping)

A navegação no FTP da ANS pode ser feita manipulando a URL ou interpretando o HTML `Index of`.

### Parâmetros de Ordenação da URL

Úteis para inspeção manual ou se quisermos forçar uma ordem de raspagem:

- `?C=N;O=D` -> Order by **N**ame (Descending)
- `?C=M;O=A` -> Order by **M**odified Date (Ascending) - _Útil para pegar o mais recente_
- `?C=S;O=A` -> Order by **S**ize

### Estrutura de Diretórios

- Raiz: `YYYY/` (Ex: `2025/`, `2024/`)
- Conteúdo: Arquivos `.zip` ou `.csv`.
- **Risco:** Alguns trimestres podem ter múltiplos arquivos (republicações).
- **Decisão:** Priorizar o arquivo com data de modificação mais recente ou processar todos e remover duplicatas via `hash` do arquivo.

---

## 4. Resumo da Pipeline (ETL)

1. **Extract (Scraper):**
   - Iterar diretórios `Demonstracoes_Contabeis`.
   - Baixar ZIPs.
   - Baixar `Relatorio_cadop.csv` (Ativas) e `Relatorio_cadop_canceladas.csv`.
2. **Transform (Processor):**
   - **Normalizar:** Resolver encoding e separadores (`;`).
   - **Limpar:** Remover duplicatas contábeis.
   - **Enriquecer:** Fazer `MERGE` (Left Join) da Contabilidade com (Ativas + Canceladas) usando `REG_ANS` como chave.
   - **Filtrar:** Buscar apenas a conta "EVENTOS/ SINISTROS CONHECIDOS OU AVISADOS DE ASSISTÊNCIA A SAÚDE MEDICO HOSPITALAR" (Filtro por texto na coluna `DESCRICAO` ou código contábil).
3. **Load:**
   - Salvar CSV final: `data/processed/demonstracoes_consolidadas.csv`.
