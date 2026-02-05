from __future__ import annotations

import os
import logging
from typing import Dict

import pandas as pd

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, "data", "processed")
CONSOLIDADO_ZIP_PATH = os.path.join(DATA_DIR, "consolidado_despesas.zip")
AGREGADO_ZIP_PATH = os.path.join(DATA_DIR, "Teste_JoseGustavo.zip")


class DataLayer:
    """
    Camada de dados em memória baseada em classe.

    Os dados são carregados uma única vez por processo e reutilizados
    durante todo o ciclo de vida da aplicação.

    Attributes:
        _df_despesas (pd.DataFrame): Tabela de fatos com despesas detalhadas.
        _df_operadoras (pd.DataFrame): Dimensão com cadastro único das operadoras.
        _df_agregado (pd.DataFrame): Dados agregados para dashboards.
        _stats (Dict): Estatísticas globais pré-calculadas.
    """

    _df_despesas: pd.DataFrame = pd.DataFrame()
    _df_operadoras: pd.DataFrame = pd.DataFrame()
    _df_agregado: pd.DataFrame = pd.DataFrame()
    _stats: Dict = {}

    @classmethod
    def load_data(cls) -> None:
        """
        Carrega todos os conjuntos de dados em memória.

        O método é idempotente e pode ser chamado múltiplas vezes
        sem efeitos colaterais.
        """
        os.makedirs(DATA_DIR, exist_ok=True)
        cls._load_consolidado()
        cls._load_agregado()

    @classmethod
    def _load_consolidado(cls) -> None:
        """
        Carrega o arquivo consolidado de despesas.
        """
        if not os.path.exists(CONSOLIDADO_ZIP_PATH):
            logger.error("Arquivo consolidado não encontrado: %s", CONSOLIDADO_ZIP_PATH)
            cls._df_despesas = pd.DataFrame()
            cls._df_operadoras = pd.DataFrame()
            return

        df = pd.read_csv(
            CONSOLIDADO_ZIP_PATH,
            sep=";",
            compression="zip",
            dtype=str,
        )

        if "ValorDespesas" in df.columns:
            df["ValorDespesas"] = pd.to_numeric(
                df["ValorDespesas"], errors="coerce"
            ).fillna(0.0)

        cls._df_despesas = df

        cols = ["RegistroANS", "CNPJ", "RazaoSocial", "UF", "Modalidade"]
        available = [c for c in cols if c in df.columns]

        cls._df_operadoras = (
            df[available].drop_duplicates(subset=["CNPJ"]).sort_values("RazaoSocial")
            if available
            else pd.DataFrame()
        )

    @classmethod
    def _load_agregado(cls) -> None:
        """
        Carrega o arquivo agregado utilizado em dashboards.
        """
        if not os.path.exists(AGREGADO_ZIP_PATH):
            cls._df_agregado = pd.DataFrame()
            cls._stats = {}
            return

        try:
            df = pd.read_csv(
                AGREGADO_ZIP_PATH,
                sep=";",
                compression="zip",
                dtype=str,
            )

            required_cols = {
                "RazaoSocial",
                "UF",
                "ValorTotal",
                "MediaTrimestral",
                "DesvioPadrao",
            }

            missing = required_cols - set(df.columns)
            if missing:
                raise ValueError(
                    f"Colunas obrigatórias ausentes no agregado: {missing}"
                )

            df["ValorTotal"] = (
                df["ValorTotal"]
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
            )

            df["ValorTotal"] = pd.to_numeric(
                df["ValorTotal"],
                errors="coerce",
            ).fillna(0.0)

            cls._df_agregado = df

            cls._stats = {
                "total_despesas": float(df["ValorTotal"].sum()),
                "total_operadoras": int(len(df)),
                "top_operadoras": df.nlargest(10, "ValorTotal")[
                    ["RazaoSocial", "ValorTotal"]
                ].to_dict(orient="records"),
                "por_uf": df.groupby("UF")["ValorTotal"].sum().to_dict(),
            }

        except Exception as exc:
            logger.exception("Erro ao carregar agregado: %s", exc)
            cls._df_agregado = pd.DataFrame()
            cls._stats = {}

    @classmethod
    def get_despesas(cls) -> pd.DataFrame:
        """
        Retorna a tabela de fatos com despesas detalhadas.

        Returns:
            pd.DataFrame: Histórico completo de despesas.
        """
        if cls._df_despesas.empty:
            cls.load_data()
        return cls._df_despesas.copy()

    @classmethod
    def get_operadoras(cls) -> pd.DataFrame:
        """
        Retorna a dimensão de operadoras únicas.

        Returns:
            pd.DataFrame: Cadastro único das operadoras.
        """
        if cls._df_operadoras.empty:
            cls.load_data()
        return cls._df_operadoras.copy()

    @classmethod
    def get_agregado(cls) -> pd.DataFrame:
        """
        Retorna a tabela agregada para dashboards.

        Returns:
            pd.DataFrame: Dados consolidados por operadora e UF.
        """
        if cls._df_agregado.empty:
            cls.load_data()
        return cls._df_agregado.copy()

    @classmethod
    def get_stats(cls) -> Dict:
        """
        Retorna estatísticas globais pré-calculadas.

        Returns:
            Dict: Estatísticas para cards e gráficos.
        """
        if not cls._stats:
            cls.load_data()
        return dict(cls._stats)
