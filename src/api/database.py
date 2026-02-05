import os
import pandas as pd
import logging
from typing import Dict

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CSV_PATH = os.path.join(BASE_DIR, "data", "processed", "consolidado_despesas.csv")


class DataLayer:
    """
    Camada de persistência em memória utilizando o padrão Singleton.

    Esta classe atua como um banco de dados de alta performance, carregando os dados
    dos arquivos CSV processados diretamente na memória RAM. A abordagem elimina
    overhead de leitura em disco durante requisições e garante compatibilidade
    tanto em ambiente local quanto em plataformas PaaS como o Render.

    Attributes:
        _df_despesas (pd.DataFrame): Tabela de fatos com o histórico financeiro.
        _df_operadoras (pd.DataFrame): Tabela de dimensão com operadoras únicas.
        _stats (Dict): Cache de estatísticas agregadas.
    """

    _df_despesas: pd.DataFrame = pd.DataFrame()
    _df_operadoras: pd.DataFrame = pd.DataFrame()
    _stats: Dict = {}

    @classmethod
    def load_data(cls) -> None:
        """
        Carrega os dados do CSV consolidado para memória.

        Garante que o caminho do arquivo seja absoluto e válido em qualquer ambiente.
        Caso o arquivo não exista, inicializa DataFrames vazios para evitar falhas
        na API. Também realiza conversões necessárias para análises numéricas.
        """
        os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)

        if not os.path.exists(CSV_PATH):
            logger.error(f"Arquivo de dados não encontrado: {CSV_PATH}")
            cls._df_despesas = pd.DataFrame(
                columns=["CNPJ", "RazaoSocial", "ValorDespesas", "UF"]
            )
            cls._df_operadoras = pd.DataFrame()
            cls._stats = {}
            return

        try:
            logger.info(f"Carregando dados de: {CSV_PATH}")
            df = pd.read_csv(CSV_PATH, sep=";", encoding="utf-8", dtype=str)

            df["ValorDespesas"] = pd.to_numeric(
                df["ValorDespesas"], errors="coerce"
            ).fillna(0.0)

            cls._df_despesas = df

            cols_op = ["RegistroANS", "CNPJ", "RazaoSocial", "UF", "Modalidade"]
            available_cols = [c for c in cols_op if c in df.columns]

            cls._df_operadoras = (
                df[available_cols]
                .drop_duplicates(subset=["CNPJ"])
                .sort_values("RazaoSocial")
            )

            cls._stats = {
                "total_despesas": float(df["ValorDespesas"].sum()),
                "total_operadoras": (
                    int(cls._df_operadoras["CNPJ"].nunique())
                    if not cls._df_operadoras.empty
                    else 0
                ),
            }

            logger.info(
                f"Dados carregados com sucesso: {len(cls._df_operadoras)} operadoras."
            )

        except Exception as e:
            logger.error(f"Erro ao processar CSV: {e}")
            cls._df_despesas = pd.DataFrame()
            cls._df_operadoras = pd.DataFrame()
            cls._stats = {}

    @classmethod
    def get_operadoras(cls) -> pd.DataFrame:
        """
        Retorna a dimensão de operadoras únicas.

        Returns:
            pd.DataFrame: Cadastro único de operadoras.
        """
        if cls._df_operadoras.empty:
            cls.load_data()
        return cls._df_operadoras

    @classmethod
    def get_despesas(cls) -> pd.DataFrame:
        """
        Retorna a tabela de fatos com todas as despesas.

        Returns:
            pd.DataFrame: Histórico completo de despesas.
        """
        if cls._df_despesas.empty:
            cls.load_data()
        return cls._df_despesas

    @classmethod
    def get_stats(cls) -> Dict:
        """
        Retorna estatísticas agregadas previamente calculadas.

        Returns:
            Dict: Dicionário contendo métricas resumidas do dataset.
        """
        if not cls._stats:
            cls.load_data()
        return cls._stats


DataLayer.load_data()
