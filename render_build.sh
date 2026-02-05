#!/usr/bin/env bash
# O comando abaixo faz o script parar se der qualquer erro (segurança)
set -o errexit

echo "Build Start: Instalando dependências do Backend (Python)..."
pip install --upgrade pip
pip install -r requirements.txt

echo "Build: Instalando dependências do Frontend (Vue.js)..."
# Entramos na pasta do frontend
cd frontend
# Instalamos os pacotes do Node
npm install
# Rodamos o build de produção (gera a pasta /dist)
npm run build
# Voltamos para a raiz
cd ..

echo "Build Success! Arquivos estáticos gerados em /frontend/dist"