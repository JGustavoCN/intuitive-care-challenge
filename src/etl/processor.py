import os
import re
import zipfile
import logging
import shutil
import pandas as pd
from typing import List, Optional
import csv
from .validator import DataValidator


class DataProcessor:
    """
    Motor de processamento ETL (Extract, Transform, Load) especializado para o domínio ANS.

    Esta classe encapsula a inteligência de negócio necessária para converter artefatos
    brutos e heterogêneos em conjuntos de dados limpos, validados e enriquecidos.
    Ela lida com desafios comuns de Engenharia de Dados, como Schema Drift (mudança de
    cabeçalhos) e inconsistências de encoding.

    Decisões de Arquitetura e Trade-offs Técnicos:
    - **Processamento em Memória (Pandas):** Justificado pelo volume de dados do teste
      (~100k a 500k linhas) que se ajusta perfeitamente à memória RAM moderna. Esta escolha
      prioriza a velocidade de processamento e a simplicidade do código (KISS) em vez de
      complexidades de processamento distribuído (como Spark).
    - **Isolamento de Estado (Temporalidade):** Utiliza um diretório temporário
      (`_temp_extraction`) para descompactação, garantindo que o processamento seja
      idempotente e não polua o diretório de dados brutos (`raw`).
    - **Robustez de Tipagem:** Implementa leitura forçada como string para garantir que
      identificadores numéricos (CNPJ/Registro ANS) não percam precisão ou formatação.

    Attributes:
        input_files (List[str]): Lista de caminhos para os arquivos capturados pelo Scraper.
        output_dir (str): Local de destino para a persistência dos arquivos CSV e ZIP finais.
        temp_dir (str): Espaço de trabalho efêmero para manipulação de arquivos extraídos.
    """

    def __init__(self, input_files: List[str], output_dir: str):
        """
        Inicializa o contexto de processamento e prepara a infraestrutura de I/O.

        Configura os diretórios de trabalho e garante um estado limpo (**Clean State**) para
        a extração temporária de arquivos.

        Estratégia de Gestão de Arquivos:
        Ao instanciar a classe, o diretório temporário (`_temp_extraction`) é recriado
        do zero (purgo total). Isso elimina resíduos de execuções anteriores, prevenindo
        contaminação de dados ou conflitos de nomes durante a descompactação dos ZIPs.

        Args:
            input_files (List[str]): Lista contendo os caminhos absolutos dos artefatos
                brutos baixados (ZIPs contábeis e CSVs cadastrais) pelo Scraper.
            output_dir (str): Caminho do diretório de destino onde os produtos finais
                (relatórios consolidados e agregados) serão persistidos.
        """
        self.input_files = input_files
        self.output_dir = output_dir
        self.temp_dir = os.path.join(output_dir, "_temp_extraction")

        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        os.makedirs(self.temp_dir, exist_ok=True)

    def _identify_period_from_filename(self, filename: str) -> tuple:
        """
        Realiza a inferência temporal (Ano/Trimestre) a partir do padrão de nomenclatura do arquivo.

        Esta função aplica expressões regulares (Regex) para extrair metadados de tempo,
        oferecendo resiliência contra inconsistências históricas na nomeação dos arquivos da ANS.
        É capaz de normalizar variações como '1T2025', '2024_3trim' ou '4t', garantindo
        que o dado seja catalogado no período correto.

        Args:
            filename (str): O nome do arquivo (ZIP ou extraído) a ser analisado.

        Returns:
            tuple: Uma tupla `(Ano, Trimestre)` contendo strings numéricas (ex: `('2023', '1')`).
            Retorna `(None, None)` caso o padrão não seja detectado.
        """
        year_match = re.search(r"(20\d{2})", filename)
        year = year_match.group(1) if year_match else None

        quarter_match = re.search(r"([1-4])(?:t|trim)", filename, re.IGNORECASE)
        quarter = quarter_match.group(1) if quarter_match else None
        return year, quarter

    def _load_dataframe_robust(
        self, file_path: str, sep: str = ";"
    ) -> Optional[pd.DataFrame]:
        """
        Carrega dados tabulares aplicando estratégias de resiliência contra inconsistências de formato e codificação.

        Este método atua como uma camada de abstração de I/O, resolvendo três problemas comuns
        em dados governamentais legados:
        1. **Polimorfismo de Formato:** Suporta transparência entre arquivos `.csv` e `.xlsx`.
        2. **Inferência de Encoding:** Tenta ler como `UTF-8` (padrão moderno), mas realiza
           um **fallback automático** para `Latin-1` (ISO-8859-1) em caso de erro de decodificação,
           comum em arquivos gerados por sistemas Windows antigos.
        3. **Preservação de Tipos:** Força a leitura como string (`dtype=str`) para evitar
           a supressão acidental de zeros à esquerda em identificadores (ex: CNPJ, Códigos ANS).

        Args:
            file_path (str): Caminho absoluto ou relativo do arquivo a ser lido.
            sep (str, optional): Delimitador de campos para arquivos CSV. Padrão: ";".

        Returns:
            Optional[pd.DataFrame]: O DataFrame carregado com sucesso, ou None caso o arquivo
            esteja corrompido, vazio ou ilegível.
        """
        try:
            if file_path.lower().endswith(".xlsx"):
                return pd.read_excel(file_path, dtype=str)
            try:
                return pd.read_csv(file_path, sep=sep, encoding="utf-8", dtype=str)
            except UnicodeDecodeError:
                return pd.read_csv(file_path, sep=sep, encoding="latin1", dtype=str)
        except Exception as e:
            logging.error(f"[ERRO] Falha ao ler {os.path.basename(file_path)}: {e}")
            return None

    def _load_cadop_master(self) -> pd.DataFrame:
        """
        Constrói o Mestre de Operadoras (Master Data) unificando cadastros de Ativas e Canceladas.

        Este método atua como um normalizador central, criando uma fonte única de verdade
        para o enriquecimento dos dados contábeis.

        Estratégias de Engenharia:
        1. **Schema Mapping:** Aplica um dicionário de tradução (`col_map`) para normalizar
           variações históricas de nomes de colunas (ex: converte 'REGISTRO_OPERADORA' ou
           'CD_NOTA' para o padrão 'RegistroANS').
        2. **Unificação:** Consolida arquivos fisicamente separados em um único DataFrame.
        3. **Deduplicação:** Garante a unicidade da chave primária (`RegistroANS`),
           prevenindo a explosão cartesiana (duplicidade de linhas) durante o Join futuro.

        Returns:
            pd.DataFrame: DataFrame normalizado contendo, no mínimo, as colunas
            ['RegistroANS', 'CNPJ', 'RazaoSocial', 'Modalidade', 'UF'].
            Retorna um DataFrame vazio caso a leitura falhe ou nenhum arquivo seja encontrado.
        """
        cadop_files = [
            f for f in self.input_files if "cadop" in os.path.basename(f).lower()
        ]
        dfs = []

        logging.info("--- Construindo Mestre de Operadoras (CADOP) ---")
        for f in cadop_files:
            logging.info(f"[LOAD] Carregando cadastro: {os.path.basename(f)}")
            df = self._load_dataframe_robust(f)
            if df is not None:
                df.columns = df.columns.str.upper().str.strip()
                dfs.append(df)

        if not dfs:
            logging.error("Nenhum arquivo CADOP encontrado!")
            return pd.DataFrame()

        cadop_master = pd.concat(dfs, ignore_index=True)
        col_map = {
            "REGISTRO_ANS": "RegistroANS",
            "REGISTRO_OPERADORA": "RegistroANS",
            "CD_NOTA": "RegistroANS",
            "CNPJ": "CNPJ",
            "RAZAO_SOCIAL": "RazaoSocial",
            "NO_RAZAO_SOCIAL": "RazaoSocial",
            "NM_RAZAO_SOCIAL": "RazaoSocial",
            "MODALIDADE": "Modalidade",
            "UF": "UF",
        }

        cadop_master = cadop_master.rename(
            columns={k: v for k, v in col_map.items() if k in cadop_master.columns}
        )

        if "RegistroANS" not in cadop_master.columns:
            logging.error(
                f"[ERRO] Coluna chave RegistroANS não encontrada. Colunas disponíveis: {cadop_master.columns.tolist()}"
            )
            return pd.DataFrame()

        try:
            cols_to_keep = ["RegistroANS", "CNPJ", "RazaoSocial", "Modalidade", "UF"]
            available = [c for c in cols_to_keep if c in cadop_master.columns]

            cadop_master = cadop_master[available].drop_duplicates(
                subset=["RegistroANS"]
            )
            logging.info(
                f"[SUCESSO] Mestre CADOP carregado: {len(cadop_master)} operadoras únicas."
            )
            return cadop_master
        except Exception as e:
            logging.error(f"[ERRO] Falha ao processar mestre CADOP: {e}")
            return pd.DataFrame()

    def process_accounting_files(self) -> pd.DataFrame:
        """
        Executa a extração, normalização e filtragem dos arquivos contábeis (Motor de Transformação).

        Este método é o núcleo do pipeline de processamento financeiro. Ele orquestra a abertura
        de arquivos comprimidos, a leitura de formatos heterogêneos (CSV/Excel) e a aplicação
        de regras de negócio para isolar despesas assistenciais.

        Fluxo de Processamento:
        1. **Inferência Temporal:** Extrai o período (Ano/Trimestre) do nome do arquivo ZIP para enriquecimento.
        2. **Extração Efêmera:** Descompacta arquivos em disco temporariamente apenas para leitura, economizando espaço.
        3. **Normalização de Schema:** Padroniza nomes de colunas (Uppercasing, remoção de acentos)
           para garantir interoperabilidade entre diferentes versões de layout da ANS.
        4. **Filtro de Negócio:** Seleciona apenas linhas contendo chaves como 'EVENTO' ou 'SINISTRO',
           descartando receitas e lançamentos irrelevantes para a análise de despesas.
        5. **Sanitização Numérica:** Converte strings formatadas no padrão brasileiro (ex: '1.000,00')
           para floats computáveis (ex: 1000.00).

        Returns:
            pd.DataFrame: Um DataFrame unificado (Stacked) contendo a série histórica de despesas.
            Schema: ['RegistroANS', 'Ano', 'Trimestre', 'VL_SALDO_FINAL'].
            Retorna um DataFrame vazio se nenhum dado for extraído ou se ocorrerem erros críticos.
        """
        all_data = []
        accounting_zips = [f for f in self.input_files if f.endswith(".zip")]

        for zip_path in accounting_zips:
            zip_name = os.path.basename(zip_path)
            year, quarter = self._identify_period_from_filename(zip_name)

            if not year or not quarter:
                logging.warning(f"[SKIP] Data não identificada no arquivo: {zip_name}")
                continue

            logging.info(f"[PROCESS] Contabilidade: {zip_name}")
            try:
                with zipfile.ZipFile(zip_path, "r") as z:
                    for filename in z.namelist():
                        if not filename.lower().endswith((".csv", ".txt", ".xlsx")):
                            continue

                        z.extract(filename, self.temp_dir)
                        file_path = os.path.join(self.temp_dir, filename)

                        df = self._load_dataframe_robust(file_path)
                        if df is None:
                            continue
                        df.columns = (
                            df.columns.str.strip()
                            .str.upper()
                            .str.replace("Ç", "C")
                            .str.replace("Ã", "A")
                        )
                        rename_map = {
                            "CD_CONTA": "CD_CONTA_CONTABIL",
                            "DESCRIÇÃO": "DESCRICAO",
                            "SALDO_FINAL": "VL_SALDO_FINAL",
                            "REG_ANS": "RegistroANS",
                        }
                        df = df.rename(columns=rename_map)

                        required = [
                            "RegistroANS",
                            "CD_CONTA_CONTABIL",
                            "DESCRICAO",
                            "VL_SALDO_FINAL",
                        ]
                        if not all(c in df.columns for c in required):
                            continue

                        mask = df["DESCRICAO"].str.contains(
                            "EVENTO|SINISTRO", case=False, na=False
                        )
                        df_filtered = df[mask].copy()

                        if not df_filtered.empty:
                            if df_filtered["VL_SALDO_FINAL"].dtype == "object":
                                df_filtered["VL_SALDO_FINAL"] = (
                                    df_filtered["VL_SALDO_FINAL"]
                                    .str.replace(".", "", regex=False)
                                    .str.replace(",", ".", regex=False)
                                )

                            df_filtered["VL_SALDO_FINAL"] = pd.to_numeric(
                                df_filtered["VL_SALDO_FINAL"], errors="coerce"
                            )

                            df_filtered["Ano"] = year
                            df_filtered["Trimestre"] = quarter

                            all_data.append(
                                df_filtered[
                                    [
                                        "RegistroANS",
                                        "Ano",
                                        "Trimestre",
                                        "VL_SALDO_FINAL",
                                    ]
                                ]
                            )

                        os.remove(file_path)
            except Exception as e:
                logging.error(f"[ERRO] Processando ZIP {zip_name}: {e}")

        if not all_data:
            return pd.DataFrame()
        return pd.concat(all_data, ignore_index=True)

    def generate_aggregation_report(self, df: pd.DataFrame):
        """
        Executa a Tarefa 2.3: Geração de painel analítico agregado e empacotamento final.

        Transforma os dados transacionais (por trimestre) em uma visão consolidada por
        Operadora e Estado (UF), calculando métricas estatísticas de desempenho e volatilidade.

        Métricas Calculadas:
        1. **ValorTotal (Sum):** KPI principal de volume financeiro.
        2. **MediaTrimestral (Mean):** Ticket médio de despesa por período.
        3. **DesvioPadrao (Std):** Indicador de volatilidade.
           *Nota:* Operadoras com apenas um registro trimestral resultam em desvio `NaN`.
           Estes casos são tratados imputando `0.0` (ausência de variação).

        Estratégia de Exportação (Localização Brasil):
        O arquivo é gerado visando compatibilidade nativa com o Excel em português:
        - **Separador:** Ponto e vírgula (`;`).
        - **Decimal:** Vírgula (`,`).
        - **Encoding:** `utf-8-sig` (BOM) para garantir a leitura correta de acentos.

        Fluxo de Saída:
        Gera o CSV, compacta-o imediatamente em um arquivo ZIP (`Teste_JoseGustavo.zip`)
        e remove o artefato CSV original para economizar espaço em disco (Cleanup).

        Args:
            df (pd.DataFrame): DataFrame validado contendo as colunas 'RazaoSocial',
                'UF' e 'ValorDespesas'.
        """
        logging.info("--- Gerando Relatório Agregado (Tarefa 2.3) ---")

        df_agg = (
            df.groupby(["RazaoSocial", "UF"])["ValorDespesas"]
            .agg(ValorTotal="sum", MediaTrimestral="mean", DesvioPadrao="std")
            .reset_index()
        )

        df_agg["DesvioPadrao"] = df_agg["DesvioPadrao"].fillna(0.0)

        df_agg = df_agg.sort_values(by="ValorTotal", ascending=False)

        filename = "despesas_agregadas.csv"
        file_path = os.path.join(self.output_dir, filename)

        logging.info(f"Salvando relatório agregado em: {file_path}")

        df_agg.to_csv(
            file_path, index=False, sep=";", decimal=",", encoding="utf-8-sig"
        )
        zip_filename = f"Teste_JoseGustavo.zip"
        zip_path = os.path.join(self.output_dir, zip_filename)

        logging.info(f"Compactando relatório agregado: {zip_path}")
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(file_path, arcname=filename)
        logging.info(f"Arquivo compactado com sucesso: {zip_path}")
        os.remove(file_path)

    def enrich_and_export(self, df_contabil: pd.DataFrame, df_cadop: pd.DataFrame):
        """
        Orquestra o estágio final do pipeline: Enriquecimento, Consolidação, Validação e Exportação.

        Este método atua como o integrador final, realizando o cruzamento (Join) entre os dados
        financeiros e cadastrais, aplicando filtros de integridade contábil e disparando as
        rotinas de auditoria de qualidade. Ao final, gera os artefatos de entrega exigidos.

        Fluxo de Processamento Detalhado:
        1. **Enriquecimento (Left Join):** Vincula os lançamentos contábeis ao mestre de operadoras
           usando o `RegistroANS`. Garante a preservação de dados financeiros mesmo sem match cadastral.
        2. **Tratamento de Nulos (Fallback):** Aplica placeholders (ex: 'NAO_ENCONTRADO') para evitar
           propagação de valores nulos em colunas críticas.
        3. **Limpeza Técnica:** Elimina registros com saldo zerado ou nulo que não impactam o balanço.
        4. **Consolidação (Agregação):** Agrupa os dados por CNPJ e Período, somando os valores
           para eliminar duplicidade visual de subcontas analíticas (Tarefa 1.3).
        5. **Auditoria de Qualidade:** Aciona o `DataValidator` para realizar o Flagging de
           registros inconsistentes (Tarefa 2.1).
        6. **Relatório Analítico:** Dispara a geração do relatório de estatísticas por UF (Tarefa 2.3).
        7. **Persistência Rigorosa:** Exporta o CSV consolidado utilizando citação forçada (`QUOTE_ALL`)
           para garantir a integridade do parser em sistemas externos.

        Args:
            df_contabil (pd.DataFrame): DataFrame contendo os lançamentos financeiros já filtrados.
            df_cadop (pd.DataFrame): DataFrame Mestre contendo os dados cadastrais consolidados.

        Raises:
            IOError: Se houver falha na escrita dos arquivos finais ou na criação do ZIP.
        """

        if df_contabil.empty or df_cadop.empty:
            logging.warning("[ALERTA] Dados insuficientes para consolidação.")
            return

        logging.info("--- Iniciando Enriquecimento (Left Join) ---")

        df_contabil["RegistroANS"] = df_contabil["RegistroANS"].astype(str).str.strip()
        df_cadop["RegistroANS"] = df_cadop["RegistroANS"].astype(str).str.strip()

        df_final = pd.merge(df_contabil, df_cadop, on="RegistroANS", how="left")

        fill_values = {
            "CNPJ": "NAO_ENCONTRADO",
            "RazaoSocial": "OPERADORA_NAO_IDENTIFICADA",
            "Modalidade": "DESCONHECIDA",
            "UF": "XX",
        }
        df_final.fillna(value=fill_values, inplace=True)

        df_final = df_final.dropna(subset=["VL_SALDO_FINAL"])
        df_final = df_final[df_final["VL_SALDO_FINAL"] != 0]

        logging.info("Consolidando (Somando) despesas por Operadora...")

        group_cols = [
            "CNPJ",
            "RazaoSocial",
            "RegistroANS",
            "Modalidade",
            "UF",
            "Trimestre",
            "Ano",
        ]

        existing_group_cols = [c for c in group_cols if c in df_final.columns]

        df_final = df_final.groupby(existing_group_cols, as_index=False, dropna=False)[
            ["VL_SALDO_FINAL"]
        ].sum()

        df_final = df_final.rename(columns={"VL_SALDO_FINAL": "ValorDespesas"})

        logging.info("--- Executando Validação de Qualidade de Dados (Validator) ---")
        df_final = DataValidator.run_quality_checks(df_final)

        invalid_count = len(df_final) - df_final["Registro_Conforme"].sum()

        if invalid_count > 0:
            logging.warning(
                f"[ATENCAO] {invalid_count} registros apresentaram inconsistencias (Mantidos com Flag)."
            )

        self.generate_aggregation_report(df_final)

        df_final = df_final.sort_values(by=["Ano", "Trimestre", "RazaoSocial"])

        logging.info(
            f"[SUCESSO] Dados consolidados finais: {len(df_final)} registros únicos."
        )

        csv_filename = "consolidado_despesas.csv"
        csv_path = os.path.join(self.output_dir, csv_filename)
        zip_filename = "consolidado_despesas.zip"
        zip_path = os.path.join(self.output_dir, zip_filename)

        logging.info(f"Gerando CSV: {csv_path}")

        df_final.to_csv(
            csv_path, index=False, sep=";", encoding="utf-8", quoting=csv.QUOTE_ALL
        )

        logging.info(f"Compactando para ZIP: {zip_path}")
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(csv_path, arcname=csv_filename)

        os.remove(csv_path)
        shutil.rmtree(self.temp_dir)
        logging.info("✅ Processo (ETL + Validação) concluído com sucesso!")

    def run(self):
        """
        Orquestra o fluxo de execução sequencial do motor de processamento.

        Este método atua como o 'Pipeline Manager', garantindo que as dependências de dados
        sejam respeitadas: primeiro carrega as dimensões (CADOP), depois os fatos
        (Contabilidade) e, por fim, executa a junção e validação.

        Fluxo de Atividades:
        1. **Extração de Metadados:** Invoca `_load_cadop_master` para preparar o de-para de CNPJ.
        2. **Ingestão Contábil:** Invoca `process_accounting_files` para transformar os ZIPs brutos.
        3. **Consolidação Final:** Dispara o `enrich_and_export` para gerar os produtos de dados.

        Raises:
            Exception: Propaga exceções de I/O ou processamento para o orquestrador principal (main.py).
        """
        df_cadop = self._load_cadop_master()
        df_contabil = self.process_accounting_files()
        self.enrich_and_export(df_contabil, df_cadop)
