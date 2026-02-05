import os
import pandas as pd
import logging
from typing import Optional


class DataLayer:
    """
    Camada de persistência em memória utilizando o padrão Singleton.

    Esta classe atua como um banco de dados de alta performance, carregando os dados
    dos arquivos CSV processados diretamente na memória RAM. Esta abordagem
    elimina o overhead de I/O de disco durante as requisições da API e simplifica
    o deploy em ambientes de nuvem como o Render.

    Attributes:
        _df_despesas (Optional[pd.DataFrame]): Tabela de fatos com o histórico financeiro completo.
        _df_operadoras (Optional[pd.DataFrame]): Tabela de dimensão com dados únicos de operadoras.
        _stats (Optional[dict]): Cache para armazenamento de estatísticas agregadas.
    """

    _df_despesas: Optional[pd.DataFrame] = None
    _df_operadoras: Optional[pd.DataFrame] = None
    _stats: Optional[dict] = None

    @classmethod
    def load_data(cls):
        """
        Carrega os arquivos CSV processados e estrutura as visões de dados em memória.

        O método localiza o arquivo consolidado na estrutura de pastas do projeto,
        realiza o parsing com tipos de dados estritos (strings) para preservar a
        integridade de identificadores e separa o conjunto em visões de 'Fatos'
        e 'Dimensões'. Caso o arquivo não exista, inicializa estruturas vazias.
        """
        base_path = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        csv_path = os.path.join(
            base_path, "data", "processed", "consolidado_despesas.csv"
        )

        if not os.path.exists(csv_path):
            logging.error(f"Arquivo de dados não encontrado: {csv_path}")
            cls._df_despesas = pd.DataFrame(
                columns=["CNPJ", "RazaoSocial", "ValorDespesas", "UF"]
            )
            cls._df_operadoras = pd.DataFrame()
            return

        logging.info("Carregando dados em memória (Pandas)...")

        df = pd.read_csv(csv_path, sep=";", encoding="utf-8", dtype=str)

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

        logging.info(
            f"Dados carregados! {len(cls._df_operadoras)} operadoras encontradas."
        )

    @classmethod
    def get_operadoras(cls) -> pd.DataFrame:
        """
        Retorna a visão de dimensão das operadoras únicas.

        Garante um retorno seguro do tipo DataFrame mesmo que os dados não
        tenham sido carregados previamente.

        Returns:
            pd.DataFrame: DataFrame contendo o cadastro único das operadoras.
        """
        if cls._df_operadoras is None:
            logging.warning("Acesso a get_operadoras antes da carga de dados.")
            return pd.DataFrame()
        return cls._df_operadoras

    @classmethod
    def get_despesas(cls) -> pd.DataFrame:
        """
        Retorna a visão de fatos contendo o histórico completo de despesas.

        Returns:
            pd.DataFrame: DataFrame com todos os lançamentos financeiros.
        """
        if cls._df_despesas is None:
            logging.warning("Acesso a get_despesas antes da carga de dados.")
            return pd.DataFrame()
        return cls._df_despesas
