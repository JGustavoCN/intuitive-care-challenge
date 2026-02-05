from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import math
import pandas as pd
from .database import DataLayer
from .models import (
    PaginacaoResponse,
    OperadoraSchema,
    EstatisticasSchema,
)

router = APIRouter()


def _get_dataframe_seguro(func_get):
    """
    Helper para garantir que o DataFrame não seja None.
    Se o DataLayer retornar None (erro de carga), retorna um DF vazio
    ou levanta erro dependendo da estratégia.
    """
    df = func_get()
    if df is None:
        # Logica de proteção: se o dado não carregou, retorna vazio ou erro 503
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
    # Proteção contra None
    df = _get_dataframe_seguro(DataLayer.get_operadoras)

    # 1. Filtragem (Busca)
    if search:
        search_lower = search.lower()
        # O Pylance reclama se não garantirmos que as colunas são string
        # Usamos astype(str) para garantir
        mask = df["RazaoSocial"].astype(str).str.lower().str.contains(
            search_lower, na=False
        ) | df["CNPJ"].astype(str).str.contains(search_lower, na=False)
        df = df[mask]

    # 2. Paginação
    total_items = len(df)
    total_pages = math.ceil(total_items / limit) if limit > 0 else 1

    start = (page - 1) * limit
    end = start + limit

    # Slice seguro
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
    df = _get_dataframe_seguro(DataLayer.get_operadoras)

    # Busca exata
    operadora = df[df["CNPJ"] == cnpj]

    if operadora.empty:
        raise HTTPException(status_code=404, detail="Operadora não encontrada")

    return operadora.iloc[0].to_dict()


@router.get("/operadoras/{cnpj}/despesas", response_model=PaginacaoResponse)
def historico_despesas(cnpj: str, page: int = 1, limit: int = 100):
    df_full = _get_dataframe_seguro(DataLayer.get_despesas)

    # Filtra despesas apenas daquele CNPJ
    df_despesas = df_full[df_full["CNPJ"] == cnpj]

    total_items = len(df_despesas)
    total_pages = math.ceil(total_items / limit) if limit > 0 else 1
    start = (page - 1) * limit
    end = start + limit

    # Seleção de colunas segura
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
    # Aqui podemos ser mais lenientes e retornar vazio em vez de erro 503
    df = DataLayer.get_despesas()
    df_ops = DataLayer.get_operadoras()

    # Se qualquer um for None ou Vazio
    if df is None or df_ops is None or df.empty:
        return {
            "total_despesas": 0.0,
            "media_trimestral": 0.0,
            "total_operadoras": 0,
            "top_5_uf": [],
        }

    # Cálculos Rápidos
    total_despesas = float(df["ValorDespesas"].sum())
    media_trimestral = float(df["ValorDespesas"].mean())
    total_operadoras = len(df_ops)

    # Agrupamento
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
