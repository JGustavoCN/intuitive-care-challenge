/*
-------------------------------------------------------------------------
TESTE TÉCNICO INTUITIVE CARE - TAREFA 3: BANCO DE DADOS
Autor: José Gustavo
Dialeto: PostgreSQL (Compatível com MySQL 8.0 com pequenas adaptações)
-------------------------------------------------------------------------
*/

-- ======================================================================
-- 3.2. DDL - DEFINIÇÃO DAS TABELAS
-- ======================================================================

/* TRADE-OFF DE NORMALIZAÇÃO: OPÇÃO B (MODELAGEM STAR SCHEMA / NORMALIZADA)

Justificativa da Escolha:
1. Integridade e Manutenção: Dados cadastrais (Razão Social, UF) são mutáveis, enquanto 
   despesas são fatos históricos imutáveis. A separação em Dimensão (Operadoras) e 
   Fato (Despesas) evita anomalias de atualização e redundância de armazenamento.
2. Performance Analítica: Consultas agregadas (SUM, AVG) na tabela de fatos são mais 
   performáticas quando a tabela é estreita, sem o overhead de I/O de strings longas.
*/

-- 1. Tabela Dimensão: Operadoras
-- Armazena os dados cadastrais únicos (Origem: Relatorio_cadop.csv)
CREATE TABLE operadoras (
    registro_ans INT PRIMARY KEY,         -- Chave Primária Natural (Código ANS)
    cnpj VARCHAR(14),                     -- VARCHAR para segurança na importação (embora CHAR(14) fosse ideal em base limpa)
    razao_social VARCHAR(255),
    modalidade VARCHAR(100),
    uf CHAR(2)
);
-- Índice para acelerar filtros geográficos (Query 2)
CREATE INDEX idx_operadoras_uf ON operadoras(uf);

-- 2. Tabela Fato: Despesas Consolidadas
-- Armazena os lançamentos financeiros trimestrais (Origem: consolidado_despesas.csv)
/* TRADE-OFF DE TIPOS DE DADOS:
- Valor Monetário: DECIMAL(15,2). 
  Justificativa: 'FLOAT' introduz erros de precisão financeira. DECIMAL garante a exatidão contábil.
- Datas: Inteiros (ano, trimestre).
  Justificativa: Granularidade trimestral facilita agrupamentos e índices.
*/
CREATE TABLE despesas_consolidadas (
    id SERIAL PRIMARY KEY,
    registro_ans INT NOT NULL,
    ano SMALLINT NOT NULL,
    trimestre SMALLINT NOT NULL,
    valor_despesas DECIMAL(15,2) NOT NULL,
    
    -- Constraint de Domínio (Qualidade de Dados)
    CONSTRAINT chk_trimestre CHECK (trimestre BETWEEN 1 AND 4),

    -- Chave Estrangeira
    CONSTRAINT fk_operadora 
        FOREIGN KEY (registro_ans) 
        REFERENCES operadoras(registro_ans)
);

-- Índice composto (Covering Index): Otimiza queries que filtram por tempo e agrupam por operadora
CREATE INDEX idx_despesas_tempo_operadora ON despesas_consolidadas(ano, trimestre, registro_ans);

-- 3. Tabela Agregada (Data Mart)
-- Tabela desnormalizada para leitura rápida (Origem: despesas_agregadas.csv)
CREATE TABLE despesas_agregadas_uf (
    razao_social VARCHAR(255),
    uf CHAR(2),
    valor_total DECIMAL(18,2),
    media_trimestral DECIMAL(15,2),
    desvio_padrao DECIMAL(15,2)
);

-- ======================================================================
-- 3.3. IMPORTAÇÃO DE DADOS E TRATAMENTO DE INCONSISTÊNCIAS
-- ======================================================================

/* ANÁLISE CRÍTICA DA IMPORTAÇÃO (ETL via Banco):

1. Integridade Referencial (FK):
   O consolidado financeiro pode conter operadoras já canceladas. É crucial carregar a tabela 
   'operadoras' com a união de Ativas + Canceladas para evitar erros de chave estrangeira.

2. Inconsistência de Decimais:
   O CSV usa vírgula (PT-BR), o SQL espera ponto (US). Utilizar tabela staging com REPLACE(valor, ',', '.')
   antes de inserir na tabela final.

3. Encoding:
   Forçar UTF-8 para preservar acentuação das Razões Sociais.

Exemplo PostgreSQL:
\COPY operadoras FROM 'data/processed/operadoras_tratadas.csv' WITH (FORMAT CSV, HEADER, ENCODING 'UTF8');
*/

-- ======================================================================
-- 3.4. QUERIES ANALÍTICAS
-- ======================================================================

-- QUERY 1: Top 5 operadoras com maior crescimento percentual (1º vs Último Trimestre)
/*
Desafio: Operadoras Intermitentes.
Solução: INNER JOIN entre Início e Fim.
Justificativa: Filtra automaticamente quem não tem dados nas duas pontas, garantindo 
cálculo sobre o período completo.
*/
WITH limites AS (
    SELECT MIN(ano * 10 + trimestre) as min_periodo,
           MAX(ano * 10 + trimestre) as max_periodo
    FROM despesas_consolidadas
),
inicio AS (
    SELECT d.registro_ans, d.valor_despesas as valor_inicial
    FROM despesas_consolidadas d
    CROSS JOIN limites l -- Sintaxe explícita (ANSI-92) mais moderna que a vírgula implícita
    WHERE (d.ano * 10 + d.trimestre) = l.min_periodo
),
fim AS (
    SELECT d.registro_ans, d.valor_despesas as valor_final
    FROM despesas_consolidadas d
    CROSS JOIN limites l
    WHERE (d.ano * 10 + d.trimestre) = l.max_periodo
)
SELECT 
    o.razao_social,
    i.valor_inicial,
    f.valor_final,
    ROUND(((f.valor_final - i.valor_inicial) / i.valor_inicial) * 100, 2) as crescimento_pct
FROM inicio i
JOIN fim f ON i.registro_ans = f.registro_ans
JOIN operadoras o ON i.registro_ans = o.registro_ans
WHERE i.valor_inicial > 0
ORDER BY crescimento_pct DESC
LIMIT 5;


-- QUERY 2: Distribuição por UF e Média por Operadora
/*
Desafio Adicional: Média por operadora vs Média por linha.
Solução: SUM(valor) / COUNT(DISTINCT registro).
*/
SELECT 
    o.uf,
    SUM(d.valor_despesas) as despesa_total_estado,
    -- Média real por CNPJ distinto no estado
    ROUND(SUM(d.valor_despesas) / NULLIF(COUNT(DISTINCT d.registro_ans), 0), 2) as media_por_operadora
FROM despesas_consolidadas d
JOIN operadoras o ON d.registro_ans = o.registro_ans
GROUP BY o.uf
ORDER BY despesa_total_estado DESC
LIMIT 5;


-- QUERY 3: Operadoras acima da média em >= 2 trimestres
/*
Trade-off Técnico: Otimização de CTE.
Abordagem: Calcular a média na CTE e filtrar diretamente no JOIN principal.
Justificativa: Elimina passos intermediários (CASE WHEN) tornando a query mais leve e legível.
*/
WITH medias_trimestrais AS (
    SELECT 
        ano, 
        trimestre, 
        AVG(valor_despesas) as media_geral_trimestre
    FROM despesas_consolidadas
    GROUP BY ano, trimestre
)
SELECT 
    o.razao_social,
    COUNT(*) as trimestres_acima_media
FROM despesas_consolidadas d
JOIN medias_trimestrais m 
    ON d.ano = m.ano AND d.trimestre = m.trimestre
JOIN operadoras o 
    ON d.registro_ans = o.registro_ans
WHERE d.valor_despesas > m.media_geral_trimestre -- Filtro direto (mais performático)
GROUP BY o.razao_social
HAVING COUNT(*) >= 2
ORDER BY trimestres_acima_media DESC, o.razao_social;