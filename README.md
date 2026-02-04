# Teste Técnico - Intuitive Care

Este repositório contém a solução para o teste técnico de estágio da Intuitive Care (v2.0). O projeto foca no processamento de dados abertos da ANS (Agência Nacional de Saúde Suplementar), demonstrando habilidades em Python, automação, limpeza de dados e documentação.

## 🚀 Visão Geral

O projeto foi construído seguindo a filosofia de **componentes desacoplados**:

1. **Scraper (Extração):** Responsável por conectar no FTP da ANS e baixar os dados brutos.
2. **Processor (Transformação):** Responsável por normalizar, limpar e consolidar os dados.

### Decisões Técnicas (Trade-offs)

- **Linguagem:** Python 3.14 (Foco em legibilidade e ecossistema de dados).
- **Gerenciamento de Pacotes:** `venv` + `requirements.txt`.
  - _Motivo:_ Abordagem KISS (Keep It Simple). Garante que qualquer avaliador consiga rodar o projeto sem precisar instalar ferramentas complexas como Poetry ou Docker, apenas o Python padrão.
- **Processamento de Dados:** Pandas com processamento em memória (com preparação para _chunking_).
  - _Motivo:_ O volume de dados da ANS para 3 trimestres cabe na memória de máquinas modernas. O uso do Pandas acelera o desenvolvimento e facilita a manipulação de colunas inconsistentes (CSV vs XLSX).

## 📂 Estrutura do Projeto

```text
/
├── .vscode/             # Configurações de ambiente (padronização de editor)
├── data/                # Armazenamento de dados (ignorado no git)
│   ├── raw/             # Arquivos ZIP e brutos baixados da ANS
│   └── processed/       # Arquivo final consolidado
├── src/                 # Código fonte da aplicação
│   ├── scraper.py       # Lógica de download e navegação em diretórios
│   └── processor.py     # Lógica de ETL (Extração, Transformação, Carga)
├── main.py              # Ponto de entrada (Entrypoint)
├── requirements.txt     # Dependências do projeto
└── README.md            # Documentação
```

## 🛠️ Como Executar

### Pré-requisitos

- Python 3.8 ou superior
- Git

### Instalação

1. Clone o repositório:

```bash

git clone https://github.com/JGustavoCN/intuitive-care-challenge.git
cd teste-intuitive-care
```

1. Crie e ative o ambiente virtual:

- **Windows:**

```bash
python -m venv venv
.\venv\Scripts\activate
```

- **Linux/Mac:**

```bash
python3 -m venv venv
source venv/bin/activate
```

1. Instale as dependências:

```bash
pip install -r requirements.txt
```

1. Execute o script principal:

```bash
python main.py
```

Os arquivos processados estarão na pasta data/processed.
