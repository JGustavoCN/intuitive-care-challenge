from typing import List, Optional, Any
from pydantic import BaseModel


# --- Modelos de Dados (O que é uma Operadora?) ---
class OperadoraSchema(BaseModel):
    RegistroANS: str
    CNPJ: str
    RazaoSocial: str
    UF: str
    Modalidade: str


class DespesaSchema(BaseModel):
    Ano: str
    Trimestre: str
    ValorDespesas: float


class EstatisticasSchema(BaseModel):
    total_despesas: float
    media_trimestral: float
    total_operadoras: int
    top_5_uf: List[dict]  # Ex: [{"UF": "SP", "total": 1000.0}]


# --- Modelos de Resposta Paginada (Envelope Pattern) ---
class MetaData(BaseModel):
    total_items: int
    page: int
    limit: int
    total_pages: int


class PaginacaoResponse(BaseModel):
    data: List[Any]
    meta: MetaData
