import os
import re
import requests
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from typing import List, Optional, Dict


class ANSScraper:
    """Cliente de extração de dados responsável por navegar e baixar arquivos do FTP da ANS.

    Esta classe implementa a lógica de crawling para identificar diretórios de anos,
    lidar com variações de nomenclatura de arquivos (via Regex) e garantir o download
    dos dados mais recentes disponíveis.

    Attributes:
        BASE_URL (str): URL raiz do servidor de dados abertos da ANS.
        CADOP_SOURCES (Dict[str, str]): Mapeamento entre diretórios do FTP e nomes de arquivos de cadastro desejados.
        FINANCIAL_DIR (str): Diretório relativo onde residem as demonstrações contábeis.
    """

    BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/"

    CADOP_SOURCES: Dict[str, str] = {
        "operadoras_de_plano_de_saude_ativas/": "Relatorio_cadop.csv",
        "operadoras_de_plano_de_saude_canceladas/": "Relatorio_cadop_canceladas.csv",
    }

    FINANCIAL_DIR = "demonstracoes_contabeis/"

    def __init__(self, output_dir: str):
        """Inicializa o Scraper configurando a sessão HTTP e o diretório de saída.

        Args:
            output_dir (str): Caminho local onde os arquivos baixados (RAW) serão salvos.
        """
        self.output_dir = output_dir
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) python-requests/IntuitiveTest"
            }
        )

    def _get_soup(self, url: str) -> Optional[BeautifulSoup]:
        """Baixa o HTML de uma página e retorna um objeto BeautifulSoup para navegação.

        Args:
            url (str): A URL completa do diretório a ser acessado.

        Returns:
            Optional[BeautifulSoup]: Objeto parseado ou None em caso de falha de conexão.
        """
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except requests.RequestException as e:
            logging.error(f"Erro ao acessar {url}: {e}")
            return None

    def _download_file(self, url: str, filename: str) -> Optional[str]:
        """Realiza o download de um arquivo em chunks para otimizar uso de memória.

        Implementa um sistema de cache simples que verifica se o arquivo já existe
        localmente antes de iniciar o download.

        Args:
            url (str): URL direta do arquivo.
            filename (str): Nome com o qual o arquivo será salvo localmente.

        Returns:
            Optional[str]: Caminho completo do arquivo salvo ou None em caso de erro.
        """
        local_path = os.path.join(self.output_dir, filename)
        if os.path.exists(local_path):
            logging.info(f"[CACHE] Arquivo ja existe: {filename}")
            return local_path

        logging.info(f"[DOWNLOAD] Baixando: {filename}...")
        try:
            with self.session.get(url, stream=True) as r:
                r.raise_for_status()
                with open(local_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            logging.info(f"[SUCESSO] Download concluido: {filename}")
            return local_path
        except Exception as e:
            logging.error(f"[ERRO] Falha ao baixar {filename}: {e}")
            if os.path.exists(local_path):
                os.remove(local_path)
            return None

    def download_cadop_data(self) -> List[str]:
        """Baixa os arquivos de cadastro de operadoras (Ativas e Canceladas).

        Estes arquivos são essenciais para a etapa de enriquecimento (Enrichment),
        pois fornecem o CNPJ e a Razão Social vinculados ao REG_ANS.

        Returns:
            List[str]: Lista de caminhos locais dos arquivos baixados.
        """
        downloaded = []
        logging.info("--- Iniciando download de dados Cadastrais (CADOP) ---")

        for dir_suffix, target_filename_part in self.CADOP_SOURCES.items():
            url = urljoin(self.BASE_URL, dir_suffix)
            soup = self._get_soup(url)

            if not soup:
                continue

            found = False
            for link in soup.find_all("a"):
                href = link.get("href")
                if not isinstance(href, str):
                    continue
                if target_filename_part.lower().replace(".csv", "") in href.lower():
                    full_url = urljoin(url, href)
                    local_filename = href

                    path = self._download_file(full_url, local_filename)
                    if path:
                        downloaded.append(path)
                        found = True
                        break

            if not found:
                logging.warning(
                    f"[ALERTA] Arquivo padrao nao encontrado em: {dir_suffix}"
                )

        return downloaded

    def run(self) -> List[str]:
        """Executa o fluxo principal de scraping.

        1. Baixa dados cadastrais (Ativas/Canceladas).
        2. Navega nos diretórios de demonstrações contábeis.
        3. Identifica e baixa os arquivos dos últimos 3 trimestres disponíveis.

        Returns:
            List[str]: Lista com todos os caminhos de arquivos baixados (Contábeis + Cadastrais)
            para serem consumidos pelo processador.
        """
        downloaded_files = []

        cadop_files = self.download_cadop_data()
        downloaded_files.extend(cadop_files)

        logging.info("--- Iniciando download das Demonstracoes Contabeis ---")
        url_contabil = urljoin(self.BASE_URL, self.FINANCIAL_DIR)
        soup_years = self._get_soup(url_contabil)

        if not soup_years:
            return downloaded_files

        years = []
        for link in soup_years.find_all("a"):
            href = link.get("href")

            if not isinstance(href, str):
                continue

            clean_href = href.strip("/")
            if clean_href.isdigit() and len(clean_href) == 4:
                years.append(clean_href)

        years.sort(reverse=True)

        quarters_found = 0
        MAX_QUARTERS = 3

        for year in years:
            if quarters_found >= MAX_QUARTERS:
                break

            logging.info(f"[INFO] Verificando ano: {year}")
            url_year = urljoin(url_contabil, f"{year}/")
            soup_files = self._get_soup(url_year)

            if not soup_files:
                continue

            for link in soup_files.find_all("a"):
                href = link.get("href")

                if not isinstance(href, str):
                    continue

                if not href.lower().endswith(".zip"):
                    continue

                if re.search(r"[1-4].*(t|trim)", href, re.IGNORECASE):
                    full_url = urljoin(url_year, href)
                    file_path = self._download_file(full_url, href)

                    if file_path:
                        downloaded_files.append(file_path)
                        quarters_found += 1
                        logging.info(
                            f"[INFO] Trimestre capturado: {href} ({quarters_found}/{MAX_QUARTERS})"
                        )

                    if quarters_found >= MAX_QUARTERS:
                        break

        return downloaded_files
