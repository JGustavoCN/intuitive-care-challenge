import re
import pandas as pd
from typing import List


class DataValidator:
    """
    Componente responsável pela validação de qualidade de dados (Data Quality).
    Aplica regras de negócio e integridade sobre o DataFrame consolidado.
    """

    @staticmethod
    def _calculate_cnpj_digit(cnpj_base: str, weights: List[int]) -> int:
        """
        Calcula um dígito verificador do CNPJ seguindo o algoritmo Módulo 11.

        Realiza a soma ponderada dos dígitos fornecidos e aplica a lógica de resto
        da divisão para determinar o dígito verificador (retorna 0 se resto < 2,
        caso contrário retorna 11 - resto).

        Args:
            cnpj_base (str): A sequência base de números (string) para o cálculo (ex: os 12 primeiros dígitos).
            weights (List[int]): Lista de pesos inteiros correspondentes para a multiplicação posicional.

        Returns:
            int: O dígito verificador calculado (um inteiro entre 0 e 9).
        """
        soma = sum(int(digit) * weight for digit, weight in zip(cnpj_base, weights))
        remainder = soma % 11
        return 0 if remainder < 2 else 11 - remainder

    @classmethod
    def validate_cnpj(cls, cnpj: str) -> bool:
        """
        Valida se um CNPJ é matematicamente válido (Algoritmo Módulo 11).

        Args:
            cnpj (str): String contendo o CNPJ (com ou sem máscara).

        Returns:
            bool: True se válido, False caso contrário.
        """
        if not isinstance(cnpj, str):
            return False
        cnpj_clean = re.sub(r"\D", "", cnpj)

        if len(cnpj_clean) != 14 or len(set(cnpj_clean)) == 1:
            return False

        weights1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        digit1 = cls._calculate_cnpj_digit(cnpj_clean[:12], weights1)

        weights2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        digit2 = cls._calculate_cnpj_digit(cnpj_clean[:13], weights2)

        return cnpj_clean[-2:] == f"{digit1}{digit2}"

    @classmethod
    def run_quality_checks(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Executa uma bateria de validações de qualidade e enriquece o DataFrame com metadados.

        Implementa a estratégia de 'Soft Validation' (Flagging), preservando os dados originais
        mas adicionando indicadores booleanos de conformidade para auditoria posterior.

        Regras aplicadas:
        1. CNPJ_Valido: Validação matemática dos dígitos verificadores (Módulo 11).
        2. RazaoSocial_Valida: Verificação de nulidade ou string vazia.
        3. Valor_Valido: Verificação de regra de negócio (apenas valores positivos).

        Args:
            df (pd.DataFrame): DataFrame consolidado contendo as colunas 'CNPJ',
            'RazaoSocial' e 'ValorDespesas'.

        Returns:
            pd.DataFrame: O DataFrame original acrescido das colunas de validação
            ('CNPJ_Valido', 'RazaoSocial_Valida', 'Valor_Valido') e da flag geral
            'Registro_Conforme'.
        """
        if df.empty:
            return df

        df["CNPJ_Valido"] = df["CNPJ"].apply(cls.validate_cnpj)

        df["RazaoSocial_Valida"] = df["RazaoSocial"].notna() & (
            df["RazaoSocial"].astype(str).str.strip() != ""
        )
        df["Valor_Valido"] = pd.to_numeric(df["ValorDespesas"], errors="coerce") > 0

        df["Registro_Conforme"] = (
            df["CNPJ_Valido"] & df["RazaoSocial_Valida"] & df["Valor_Valido"]
        )

        return df
