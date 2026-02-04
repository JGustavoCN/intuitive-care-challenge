# Teste Técnico - Intuitive Care

Este repositório contém a solução para o teste técnico de estágio da Intuitive Care (v2.0). O projeto foca no processamento de dados abertos da ANS (Agência Nacional de Saúde Suplementar), demonstrando habilidades em Python, automação, limpeza de dados e documentação técnica.

## 🚀 Visão Geral e Filosofia

O projeto foi construído seguindo a filosofia de **componentes desacoplados** e foco na **Experiência de Desenvolvimento (DX)**.

### 1. Arquitetura

- **Scraper (Extração):** Componente isolado responsável por navegar no FTP da ANS e baixar dados brutos.
- **Processor (Transformação):** Componente responsável pela normalização (tratamento de inconsistências) e consolidação (ETL).

### 2. Decisões Técnicas (Trade-offs)

- **Gerenciamento de Dependências (KISS):** Optei pelo uso padrão de `venv` + `requirements.txt`.
  - _Justificativa:_ Evita a necessidade de o avaliador instalar ferramentas externas (como Poetry ou Docker). A simplicidade reduz o atrito para execução imediata.
- **Processamento de Dados:** Pandas com processamento em memória.
  - _Justificativa:_ O volume de dados de 3 trimestres cabe confortavelmente na memória de máquinas modernas. O Pandas oferece a melhor relação entre performance de desenvolvimento e capacidade de manipulação de dados "sujos" (encoding e delimitadores variados).
- **Padronização de Ambiente (.vscode):** O projeto inclui configurações de editor.
  - _Justificativa:_ Garante que qualquer desenvolvedor tenha a mesma formatação (Black), linting e configurações de debug ao abrir o projeto, eliminando "conflitos de configuração".

## 📂 Estrutura do Projeto

```text
/
├── .vscode/             # ⚙️ A mágica da DX (Configurações, Tasks e Launchers)
├── data/                # Armazenamento de dados (ignorado no git)
│   ├── raw/             # Arquivos ZIP originais baixados da ANS
│   └── processed/       # Arquivo final consolidado e limpo
├── src/                 # Código fonte da aplicação
│   ├── scraper.py       # Lógica de download (Crawler)
│   └── processor.py     # Lógica de ETL e limpeza
├── main.py              # Ponto de entrada (Entrypoint)
├── requirements.txt     # Dependências do projeto
└── README.md            # Documentação

```

## 🛠️ Como Executar (Developer Experience)

Este projeto foi otimizado para o **VS Code**. Siga os passos abaixo para a melhor experiência.

### Pré-requisitos

- Python 3.8 ou superior
- Git

### Instalação

1. **Clone o repositório:**

```bash
git clone [https://github.com/JGustavoCN/intuitive-care-challenge.git](https://github.com/JGustavoCN/intuitive-care-challenge.git)
cd intuitive-care-challenge

```

1. **Abra no VS Code:**

```bash
code .
```

> 💡 **Dica Pro:** Ao abrir o projeto, o VS Code pode exibir um pop-up no canto inferior direito: _"Do you want to install the recommended extensions for this repository?"_. Clique em **Install**. Isso garantirá que você tenha as ferramentas de Python e formatação corretas.

1. **Crie o Ambiente Virtual:**

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

1. **Instale as Dependências:**

- **Via Terminal:**

```bash
pip install -r requirements.txt

```

- **Via VS Code Task (Alternativa):**
  Pressione `Ctrl + Shift + B` (ou `Cmd + Shift + B`) e selecione "Instalar Dependências". O VS Code fará isso automaticamente para você.

### Execução

Você tem duas opções para rodar o projeto:

1. **Modo Debug (F5):**
   Apenas pressione **F5** no seu teclado. O arquivo `launch.json` já está configurado para iniciar o `main.py` no terminal integrado.
2. **Modo Terminal:**

```bash
python main.py

```

### Resultados

Após a execução, verifique a pasta `data/`:

- Os zips baixados estarão em `data/raw`.
- O arquivo final consolidado estará em `data/processed`.

---

_Desenvolvido como parte do processo seletivo da Intuitive Care._
