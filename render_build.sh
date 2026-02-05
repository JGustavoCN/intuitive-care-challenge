#!/usr/bin/env bash
set -o errexit

echo "Build Start: Instalando dependências..."
pip install --upgrade pip
pip install -r requirements.txt

# Garante que as pastas existam antes do script rodar
mkdir -p data/raw data/processed

echo "Executando Pipeline de Dados..."
python run_pipeline.py

# Dá permissão de leitura para os arquivos gerados
chmod -R 755 data/

echo "Build: Frontend..."
cd frontend
npm install
npm run build
cd ..

echo "Build Success!"