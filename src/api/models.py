from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field


class OperadoraSchema(BaseModel):
    """
    Representa o cadastro simplificado de uma operadora de saúde.

    Attributes:
        RegistroANS (str): Identificador único da operadora na Agência Nacional de Saúde.
        CNPJ (str): Cadastro Nacional da Pessoa Jurídica (identificador fiscal).
        RazaoSocial (str): Nome empresarial da operadora.
        UF (str): Unidade Federativa da sede da operadora.
        Modalidade (str): Segmentação da operadora (ex: Filantropia, Autogestão).
    """

    RegistroANS: str
    CNPJ: str
    RazaoSocial: str
    UF: str
    Modalidade: str


class DespesaSchema(BaseModel):
    """
    Representa um lançamento consolidado de despesas assistenciais.

    Attributes:
        Ano (str): Exercício financeiro do lançamento.
        Trimestre (str): Período trimestral correspondente (1 a 4).
        ValorDespesas (float): Montante total de despesas (Eventos/Sinistros) no período.
    """

    Ano: str
    Trimestre: str
    ValorDespesas: float


class EstatisticasSchema(BaseModel):
    """
    Conjunto de métricas agregadas para o dashboard analítico.

    Attributes:
        total_despesas (float): Somatório global de despesas no dataset filtrado.
        media_trimestral (float): Média aritmética das despesas por período.
        total_operadoras (int): Contagem de operadoras únicas processadas.
        top_5_uf (List[Dict[str, Any]]): Ranking das 5 UFs com maior volume financeiro.
    """

    total_despesas: float
    media_trimestral: float
    total_operadoras: int
    top_5_uf: List[Dict[str, Any]]


class MetaData(BaseModel):
    """
    Metadados de controle para navegação em listas paginadas.

    Attributes:
        total_items (int): Quantidade total de registros encontrados no servidor.
        page (int): Índice da página atual.
        limit (int): Quantidade de itens por página.
        total_pages (int): Total de páginas disponíveis para os parâmetros atuais.
    """

    total_items: int
    page: int
    limit: int
    total_pages: int


class PaginacaoResponse(BaseModel):
    """
    Envelope padrão para respostas da API que envolvem múltiplas entidades.

    Implementa o 'Envelope Pattern', separando os dados brutos (data) das
    informações de contexto (meta), facilitando a implementação de tabelas
    dinâmicas no frontend.

    Attributes:
        data (List[Any]): Lista de objetos (Operadoras, Despesas, etc).
        meta (MetaData): Informações de suporte para a paginação.
    """

    data: List[Any]
    meta: MetaData
