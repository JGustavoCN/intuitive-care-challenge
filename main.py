import os
import logging
from src import ANSScraper, DataProcessor

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    """Orquestrador principal do Teste Técnico da Intuitive Care.

    Este script executa o pipeline de dados completo (ETL) exigido no teste:
    1. Scraping: Baixa os dados brutos da ANS (Demonstrações Contábeis e Cadastros).
    2. Processing: Normaliza, limpa e padroniza os arquivos.
    3. Consolidation: Enriquece os dados (Join), trata inconsistências e exporta o ZIP final.

    O processo segue uma arquitetura linear e síncrona, utilizando o sistema de arquivos
    local para troca de dados entre os estágios (raw -> processed).
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
