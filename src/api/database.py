import os
import pandas as pd
import logging


class DataLayer:
    """
    Singleton que mantém os DataFrames em memória.
    Simula um banco de dados de leitura extremamente rápida.
    """

    _df_despesas = None
    _df_operadoras = None
    _stats = None

    @classmethod
    def load_data(cls):
        """Lê os CSVs processados e prepara as visões de dados."""
        # Caminho relativo: sobe de src/api/ para raiz e entra em data/processed
        base_path = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        csv_path = os.path.join(
            base_path, "data", "processed", "consolidado_despesas.csv"
        )

        if not os.path.exists(csv_path):
            logging.error(f"Arquivo de dados não encontrado: {csv_path}")
            # Cria DataFrames vazios para evitar crash
            cls._df_despesas = pd.DataFrame(
                columns=["CNPJ", "RazaoSocial", "ValorDespesas", "UF"]
            )
            cls._df_operadoras = pd.DataFrame()
            return

        logging.info("Carregando dados em memória (Pandas)...")

        # Leitura otimizada
        df = pd.read_csv(csv_path, sep=";", encoding="utf-8", dtype=str)

        # Conversão de tipos
        df["ValorDespesas"] = pd.to_numeric(
            df["ValorDespesas"], errors="coerce"
        ).fillna(0.0)

        # 1. Tabela de Fatos (Histórico Completo)
        cls._df_despesas = df

        # 2. Tabela de Dimensão (Operadoras Únicas)
        # Removemos duplicatas para ter apenas a lista de empresas
        cols_op = ["RegistroANS", "CNPJ", "RazaoSocial", "UF", "Modalidade"]
        # Garantir que colunas existem antes de filtrar
        available_cols = [c for c in cols_op if c in df.columns]
        cls._df_operadoras = (
            df[available_cols]
            .drop_duplicates(subset=["CNPJ"])
            .sort_values("RazaoSocial")
        )

        logging.info(
            f"Dados carregados! {len(cls._df_operadoras)} operadoras encontradas."
        )

    @classmethod
    def get_operadoras(cls):
        return cls._df_operadoras

    @classmethod
    def get_despesas(cls):
        return cls._df_despesas
