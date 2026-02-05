from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os
import logging

from .database import DataLayer
from .routes import router

# Configuração de Logs
logging.basicConfig(level=logging.INFO)

# Inicialização do App
app = FastAPI(
    title="Intuitive Care API",
    description="API de consulta de despesas da ANS (Teste Técnico)",
    version="1.0.0",
)

# --- 1. Configuração de CORS (Importante para o Frontend Local) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Em produção, pode restringir se quiser
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- 2. Eventos de Ciclo de Vida ---
@app.on_event("startup")
def startup_event():
    """Carrega os CSVs para a memória ao iniciar o servidor."""
    DataLayer.load_data()


# --- 3. Registro de Rotas ---
app.include_router(router, prefix="/api", tags=["Operadoras"])

# --- 4. Servir Frontend (Para o Deploy no Render) ---
# Verifica se a pasta do build existe (só existirá após rodar npm run build)
frontend_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "frontend", "dist"
)

if os.path.exists(frontend_path):
    # Serve arquivos estáticos (JS, CSS, Imagens)
    app.mount(
        "/assets",
        StaticFiles(directory=os.path.join(frontend_path, "assets")),
        name="assets",
    )

    # Rota Catch-All: Qualquer rota não-API retorna o index.html do Vue (SPA)
    @app.get("/{full_path:path}", include_in_schema=False)
    async def serve_vue_app(full_path: str):
        return FileResponse(os.path.join(frontend_path, "index.html"))


@app.get("/", include_in_schema=False)
async def root():
    if os.path.exists(frontend_path):
        return FileResponse(os.path.join(frontend_path, "index.html"))
    return {"message": "API rodando! O Frontend ainda não foi compilado."}
