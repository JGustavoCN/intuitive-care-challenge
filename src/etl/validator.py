import re
import pandas as pd
from typing import List


class DataValidator:
    """
    Módulo de validação e garantia de qualidade de dados (Data Quality Assurance).

    Esta classe atua como um motor de regras centralizado, responsável por auditar
    a integridade e a conformidade dos dados consolidados antes da exportação final.

    Estratégia de Validação:
    Adota uma abordagem não-destrutiva (**Soft Validation**). Em vez de interromper o pipeline
    ou descartar registros inconsistentes (o que geraria divergências contábeis no saldo final),
    a classe apenas anexa metadados (`flags`) indicando o status de cada registro.
    Isso preserva a informação original para fins de auditoria e rastreabilidade.

    Escopo de Regras:
    1. **Fiscal:** Verificação algorítmica de CNPJs (Padrão Receita Federal/Módulo 11).
    2. **Cadastral:** Verificação de completude (Campos obrigatórios preenchidos).
    3. **Contábil:** Consistência de sinais (Validação de valores positivos em despesas).
    """

    @staticmethod
    def _calculate_cnpj_digit(cnpj_base: str, weights: List[int]) -> int:
        """
        Calcula um dígito verificador individual utilizando o algoritmo Módulo 11.

        Esta função implementa a lógica padrão da Receita Federal: realiza a soma do produto
        escalar entre os dígitos e seus pesos, calcula o resto da divisão por 11 e aplica
        a regra de exceção (resto < 2 torna-se 0).

        Args:
            cnpj_base (str): A sequência de dígitos (string) sobre a qual o cálculo será
                aplicado (ex: os 12 primeiros dígitos para calcular o 1º DV).
            weights (List[int]): Vetor de pesos decrescentes alinhado à sequência base.

        Returns:
            int: O dígito verificador calculado (0 a 9).
        """
        soma = sum(int(digit) * weight for digit, weight in zip(cnpj_base, weights))
        remainder = soma % 11
        return 0 if remainder < 2 else 11 - remainder

    @classmethod
    def validate_cnpj(cls, cnpj: str) -> bool:
        """
        Verifica a validade matemática de um CNPJ (Cadastro Nacional da Pessoa Jurídica).

        Este método executa um pipeline completo de verificação:
        1. **Sanitização:** Remove caracteres não numéricos (pontuação/máscaras).
        2. **Blacklist:** Rejeita sequências de números repetidos (ex: '000...00') que,
           embora possam passar no cálculo matemático, são juridicamente inválidas.
        3. **Cálculo de DV:** Recalcula os dois dígitos verificadores finais com base no
           corpo do CNPJ e compara com a entrada fornecida.

        Args:
            cnpj (str): A string do CNPJ, podendo conter formatação (ex: '12.345.678/0001-90')
                ou apenas números.

        Returns:
            bool: True se o CNPJ for autêntico e respeitar o algoritmo, False caso contrário
            (incluindo entradas nulas ou com tamanho incorreto).
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
        """
        Executa uma bateria de validações de qualidade e enriquece o DataFrame com metadados de auditoria.

        Implementa a estratégia de **'Soft Validation'** (Flagging): em vez de descartar registros
        inválidos (o que causaria perda de informação financeira), o sistema apenas marca as linhas
        com indicadores booleanos. Isso permite que a equipe de dados rastreie a origem das
        inconsistências (Audit Trail).

        Regras Aplicadas:
        1. **CNPJ_Valido:** Validação matemática dos dígitos verificadores (Algoritmo Módulo 11).
        2. **RazaoSocial_Valida:** Verificação de integridade (não nulo e não vazia).
        3. **Valor_Valido:** Regra de negócio contábil (apenas valores positivos são considerados despesas válidas).

        Args:
            df (pd.DataFrame): DataFrame consolidado contendo, no mínimo, as colunas
                'CNPJ', 'RazaoSocial' e 'ValorDespesas'.

        Returns:
            pd.DataFrame: O mesmo objeto DataFrame de entrada, acrescido das colunas de validação:
            - `CNPJ_Valido` (bool)
            - `RazaoSocial_Valida` (bool)
            - `Valor_Valido` (bool)
            - `Registro_Conforme` (bool): Flag global (True apenas se todas as validações passarem).
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
