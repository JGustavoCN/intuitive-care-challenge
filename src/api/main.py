import os
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from .database import DataLayer
from .routes import router


logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="Intuitive Care API",
    description="API de consulta de despesas da ANS (Teste Técnico)",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup_event():
    """
    Inicializa os recursos críticos no startup do servidor.

    Este hook de evento garante que a carga de dados do CSV para a memória RAM (DataLayer)
    ocorra antes que o servidor comece a aceitar conexões, evitando que as primeiras
    requisições falhem por falta de dados (Warm-up).
    """
    DataLayer.load_data()


app.include_router(router, prefix="/api", tags=["Operadoras"])

frontend_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "frontend", "dist"
)

if os.path.exists(frontend_path):
    app.mount(
        "/assets",
        StaticFiles(directory=os.path.join(frontend_path, "assets")),
        name="assets",
    )

    @app.get("/{full_path:path}", include_in_schema=False)
    async def serve_vue_app(full_path: str):
        """
        Implementa a estratégia de roteamento 'Catch-All' para Single Page Application (SPA).

        Redireciona todas as rotas que não correspondem aos endpoints da API para o
        arquivo 'index.html' do Vue.js. Isso permite que o roteamento do lado do cliente
        (Vue Router) funcione corretamente após o carregamento inicial.

        Args:
            full_path (str): O caminho da URL requisitada pelo navegador.

        Returns:
            FileResponse: O arquivo index.html do build de produção do frontend.
        """
        return FileResponse(os.path.join(frontend_path, "index.html"))


@app.get("/", include_in_schema=False)
async def root():
    """
    Endpoint raiz para verificação de integridade (Health Check).

    Returns:
        FileResponse ou dict: Retorna o frontend se disponível, ou uma mensagem
        de status indicando que a API está operacional.
    """
    if os.path.exists(frontend_path):
        return FileResponse(os.path.join(frontend_path, "index.html"))
    return {"message": "API rodando! O Frontend ainda não foi compilado."}
