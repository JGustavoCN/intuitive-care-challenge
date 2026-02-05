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
    """Motor de processamento ETL (Extract, Transform, Load) para dados da ANS.

    Esta classe é responsável por transformar os dados brutos (arquivos ZIP baixados)
    em um dataset consolidado e enriquecido.

    Decisões de Arquitetura (Trade-offs):
    - Processamento em Memória (Pandas): Escolhido devido ao volume de dados (~100k linhas)
      caber confortavelmente na RAM, priorizando velocidade de desenvolvimento sobre escalabilidade infinita.
    - Estratégia de Cache: Arquivos temporários são extraídos em `_temp_extraction` e limpos após uso.
    """

    def __init__(self, input_files: List[str], output_dir: str):
        """Inicializa o processador de dados.

        Args:
            input_files (List[str]): Lista de caminhos completos dos arquivos baixados (ZIPs e CSVs).
            output_dir (str): Diretório onde o arquivo final (ZIP consolidado) será salvo.
        """
        self.input_files = input_files
        self.output_dir = output_dir
        self.temp_dir = os.path.join(output_dir, "_temp_extraction")

        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        os.makedirs(self.temp_dir, exist_ok=True)

    def _identify_period_from_filename(self, filename: str) -> tuple:
        """Deriva o Ano e Trimestre baseando-se no nome do arquivo (Resiliência).

        Utiliza Expressões Regulares (Regex) para identificar padrões como:
        '1T2025', '2024_3trim', '4t-2023', garantindo suporte a nomenclaturas legadas.

        Args:
            filename (str): Nome do arquivo ZIP.

        Returns:
            tuple: (Ano, Trimestre) ou (None, None) se não identificado.
        """
        year_match = re.search(r"(20\d{2})", filename)
        year = year_match.group(1) if year_match else None

        quarter_match = re.search(r"([1-4])(?:t|trim)", filename, re.IGNORECASE)
        quarter = quarter_match.group(1) if quarter_match else None
        return year, quarter

    def _load_dataframe_robust(
        self, file_path: str, sep: str = ";"
    ) -> Optional[pd.DataFrame]:
        """Carrega arquivos tabulares lidando com inconsistências de formato e encoding.

        Estratégia de Resiliência:
        1. Verifica extensão (.xlsx vs .csv).
        2. Tenta encoding 'utf-8' (Padrão moderno).
        3. Fallback para 'latin1' (Comum em sistemas governamentais legados).

        Args:
            file_path (str): Caminho do arquivo.
            sep (str, optional): Delimitador do CSV. Padrão é ";".

        Returns:
            Optional[pd.DataFrame]: DataFrame carregado ou None em caso de erro crítico.
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
        """Constrói o Mestre de Operadoras unificando 'Ativas' e 'Canceladas'.

        Realiza a normalização de colunas (Schema Drift) mapeando variações como
        'REGISTRO_OPERADORA' para 'REG_ANS'.

        Returns:
            pd.DataFrame: DataFrame contendo ['REG_ANS', 'CNPJ', 'RazaoSocial'] deduplicado.
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
        Executa a extração, normalização e filtragem dos arquivos contábeis (Item 1.2).

        Este método é o núcleo da transformação dos dados financeiros. Ele itera sobre
        os arquivos ZIP baixados, extrai o conteúdo tabular e aplica as regras de negócio
        para isolar as despesas assistenciais.

        Etapas do Processo:
        1. Identificação Temporal: Extrai Ano/Trimestre do nome do arquivo ZIP via Regex.
        2. Extração Segura: Descompacta arquivos temporariamente em disco (suporte a xlsx/csv).
        3. Normalização de Schema: Padroniza nomes de colunas (ex: 'REG_ANS' -> 'RegistroANS')
           e remove acentuação para evitar erros de encoding.
        4. Filtro de Negócio: Seleciona apenas linhas contendo 'EVENTO' ou 'SINISTRO'.
        5. Sanitização Numérica: Converte valores monetários do formato brasileiro
           ('1.000,00') para float padrão ('1000.00').

        Returns:
            pd.DataFrame: DataFrame empilhado (stacked) contendo as colunas:
            ['RegistroANS', 'Ano', 'Trimestre', 'VL_SALDO_FINAL']. Retorna DataFrame
            vazio caso nenhum dado seja processado.
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
        Executa a Tarefa 2.3: Agregação por UF, Estatísticas e Exportação.

        Calcula:
        - Total de Despesas
        - Média Trimestral
        - Desvio Padrão (Volatilidade)
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
        Executa o estágio final do pipeline: Enriquecimento, Consolidação, Validação, Agregação e Exportação.

        Este método orquestra a transformação final dos dados, aplicando regras de negócio
        e garantindo a integridade do dataset antes da persistência.

        Fluxo de Processamento:
        1. Enriquecimento (Join): Cruza dados contábeis com cadastro de operadoras (Ativas/Canceladas).
        2. Limpeza Técnica: Remove lançamentos nulos ou zerados que não impactam o saldo.
        3. Consolidação (Agregação): Agrupa e soma valores por CNPJ/Trimestre para eliminar duplicidade de subcontas.
        4. Validação de Qualidade (Data Quality): Aplica regras de negócio via `DataValidator` (CNPJ, Razão Social, Valores)
           e marca registros inconsistentes (Flagging) sem descartá-los.
        5. Exportação: Gera arquivo final compactado (.zip) com formatação rigorosa (CSV com aspas forçadas).

        Args:
            df_contabil (pd.DataFrame): DataFrame contendo os lançamentos financeiros filtrados.
            df_cadop (pd.DataFrame): DataFrame Mestre contendo dados cadastrais de operadoras.
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
        """Orquestra o fluxo de execução do processador."""
        df_cadop = self._load_cadop_master()
        df_contabil = self.process_accounting_files()
        self.enrich_and_export(df_contabil, df_cadop)
