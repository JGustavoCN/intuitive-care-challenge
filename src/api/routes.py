from fastapi import APIRouter, HTTPException, Query
from typing import Optional, Callable
import math
import pandas as pd
from .database import DataLayer
from .models import (
    PaginacaoResponse,
    OperadoraSchema,
    EstatisticasSchema,
)

router = APIRouter()


def _get_dataframe_seguro(func_get: Callable) -> pd.DataFrame:
    """
    Garante a integridade do acesso aos dados em memória.

    Atua como um wrapper de segurança para as chamadas ao DataLayer. Caso os dados
    não tenham sido carregados ou o objeto esteja nulo, interrompe a requisição
    com um status de indisponibilidade de serviço.

    Args:
        func_get (Callable): Método getter do DataLayer (ex: get_operadoras).

    Returns:
        pd.DataFrame: O DataFrame solicitado.

    Raises:
        HTTPException: Erro 503 caso o DataFrame esteja inacessível.
    """
    df = func_get()
    if df is None:
        raise HTTPException(
            status_code=503,
            detail="Os dados ainda estão sendo carregados ou houve erro na leitura.",
        )
    return df


@router.get("/operadoras", response_model=PaginacaoResponse)
def listar_operadoras(
    page: int = Query(1, ge=1, description="Número da página"),
    limit: int = Query(10, ge=1, le=100, description="Itens por página"),
    search: Optional[str] = Query(None, description="Busca por Razão Social ou CNPJ"),
):
    """
    Lista operadoras cadastradas com suporte a busca textual e paginação.

    Aplica filtros de substring insensíveis a maiúsculas/minúsculas nas colunas
    de Razão Social e CNPJ, realizando o fatiamento (slicing) dos dados para
    otimizar o payload de resposta.

    Args:
        page (int): Índice da página de resultados.
        limit (int): Quantidade máxima de registros por resposta.
        search (str, optional): Termo de busca para filtragem.

    Returns:
        PaginacaoResponse: Objeto contendo a lista de operadoras e metadados de página.
    """
    df = _get_dataframe_seguro(DataLayer.get_operadoras)

    if search:
        search_lower = search.lower()
        mask = df["RazaoSocial"].astype(str).str.lower().str.contains(
            search_lower, na=False
        ) | df["CNPJ"].astype(str).str.contains(search_lower, na=False)
        df = df[mask]

    total_items = len(df)
    total_pages = math.ceil(total_items / limit) if limit > 0 else 1

    start = (page - 1) * limit
    end = start + limit

    data = df.iloc[start:end].to_dict(orient="records")

    return {
        "data": data,
        "meta": {
            "total_items": total_items,
            "page": page,
            "limit": limit,
            "total_pages": total_pages,
        },
    }


@router.get("/operadoras/{cnpj}", response_model=OperadoraSchema)
def detalhes_operadora(cnpj: str):
    """
    Recupera o perfil detalhado de uma operadora específica via CNPJ.

    Args:
        cnpj (str): CNPJ da operadora alvo (apenas números ou formatado).

    Returns:
        OperadoraSchema: Dados cadastrais da operadora encontrada.

    Raises:
        HTTPException: Erro 404 caso o CNPJ não conste na base de dados.
    """
    df = _get_dataframe_seguro(DataLayer.get_operadoras)
    operadora = df[df["CNPJ"] == cnpj]

    if operadora.empty:
        raise HTTPException(status_code=404, detail="Operadora não encontrada")

    return operadora.iloc[0].to_dict()


@router.get("/operadoras/{cnpj}/despesas", response_model=PaginacaoResponse)
def historico_despesas(cnpj: str, page: int = 1, limit: int = 100):
    """
    Consulta a série histórica de despesas assistenciais de uma operadora.

    Realiza o cruzamento de dados na tabela de fatos para extrair a evolução
    financeira trimestral vinculada ao CNPJ informado.

    Args:
        cnpj (str): CNPJ da operadora.
        page (int): Página atual do histórico.
        limit (int): Limite de registros por página.

    Returns:
        PaginacaoResponse: Lista de despesas trimestrais com metadados de paginação.
    """
    df_full = _get_dataframe_seguro(DataLayer.get_despesas)
    df_despesas = df_full[df_full["CNPJ"] == cnpj]

    total_items = len(df_despesas)
    total_pages = math.ceil(total_items / limit) if limit > 0 else 1
    start = (page - 1) * limit
    end = start + limit

    cols = ["Ano", "Trimestre", "ValorDespesas"]
    data = df_despesas.iloc[start:end][cols].to_dict(orient="records")

    return {
        "data": data,
        "meta": {
            "total_items": total_items,
            "page": page,
            "limit": limit,
            "total_pages": total_pages,
        },
    }


@router.get("/estatisticas", response_model=EstatisticasSchema)
def obter_estatisticas():
    """
    Gera indicadores analíticos globais sobre o mercado de saúde suplementar.

    Calcula em tempo real métricas de volume financeiro total, média de gastos
    por período e um ranking geográfico (Top 5 UFs) baseado no montante de despesas.

    Returns:
        EstatisticasSchema: Sumário estatístico consolidado.
    """
    df = DataLayer.get_despesas()
    df_ops = DataLayer.get_operadoras()

    if df is None or df_ops is None or df.empty:
        return {
            "total_despesas": 0.0,
            "media_trimestral": 0.0,
            "total_operadoras": 0,
            "top_5_uf": [],
        }

    total_despesas = float(df["ValorDespesas"].sum())
    media_trimestral = float(df["ValorDespesas"].mean())
    total_operadoras = len(df_ops)

    top_uf = (
        df.groupby("UF")["ValorDespesas"]
        .sum()
        .sort_values(ascending=False)
        .head(5)
        .reset_index()
    )

    top_uf_dict = top_uf.rename(columns={"ValorDespesas": "total"}).to_dict(
        orient="records"
    )

    return {
        "total_despesas": total_despesas,
        "media_trimestral": media_trimestral,
        "total_operadoras": total_operadoras,
        "top_5_uf": top_uf_dict,
    }
