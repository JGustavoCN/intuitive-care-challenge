import os
import logging
from src.etl import ANSScraper, DataProcessor

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    """
    Orquestrador principal do Teste Técnico da Intuitive Care (Entrypoint).

    Este script gerencia o ciclo de vida completo do pipeline ETL (Extract, Transform, Load),
    garantindo que as dependências entre as etapas sejam respeitadas e que o ambiente
    de execução esteja corretamente configurado.

    Fluxo de Execução:
    1. **Setup de Infraestrutura:** Criação idempotente dos diretórios de dados (`data/raw`, `data/processed`).
    2. **Extração (Scraping):** Instancia o `ANSScraper` para baixar demonstrações contábeis e cadastros.
    3. **Processamento (ETL):** Instancia o `DataProcessor` para normalizar, limpar e consolidar os dados.

    Decisões de Arquitetura:
    - **Execução Síncrona:** As etapas rodam sequencialmente para garantir integridade.
    - **Persistência em Disco:** Utiliza o sistema de arquivos para troca de dados entre estágios,
      facilitando auditoria e debug.

    Raises:
        PermissionError: Se não houver permissão de escrita nos diretórios de dados.
        Exception: Exceções genéricas capturadas pelos módulos internos são logadas.
    """
    logging.info("=== Iniciando Teste Intuitive Care ===")

    base_dir = os.path.dirname(os.path.abspath(__file__))
    raw_dir = os.path.join(base_dir, "data", "raw")
    processed_dir = os.path.join(base_dir, "data", "processed")

    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)

    logging.info("--- Etapa 1: Scraping e Coleta de Dados ---")
    scraper = ANSScraper(output_dir=raw_dir)
    downloaded_files = scraper.run()

    if not downloaded_files:
        logging.warning("Nenhum arquivo encontrado ou baixado. Encerrando pipeline.")
        return

    logging.info("--- Etapa 2: ETL (Processamento, Enriquecimento e Exportação) ---")
    processor = DataProcessor(input_files=downloaded_files, output_dir=processed_dir)

    processor.run()

    logging.info("=== Pipeline Finalizado com Sucesso ===")


if __name__ == "__main__":
    main()
