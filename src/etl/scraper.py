import os
import re
import requests
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from typing import List, Optional, Dict


class ANSScraper:
    """
    Agente de extração (Crawler) especializado na navegação e coleta de dados do repositório da ANS.

    Esta classe encapsula a complexidade de interação com a estrutura de diretórios do servidor
    de dados abertos (semelhante a um FTP via HTTP). Ela implementa estratégias de robustez,
    identificação de arquivos via Expressões Regulares (Regex) e lógica de navegação temporal
    para capturar apenas os dados mais recentes exigidos pelo teste.

    Attributes:
        BASE_URL (str): Endpoint raiz (Entrypoint) do servidor de dados abertos da ANS.
        CADOP_SOURCES (Dict[str, str]): Dicionário de configuração que mapeia os diretórios
            de cadastro (Chave) para os padrões de nomes de arquivos esperados (Valor).
            Utilizado para localizar dados de operadoras Ativas e Canceladas.
        FINANCIAL_DIR (str): Caminho relativo do diretório contendo as demonstrações contábeis.
    """

    BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/"

    CADOP_SOURCES: Dict[str, str] = {
        "operadoras_de_plano_de_saude_ativas/": "Relatorio_cadop.csv",
        "operadoras_de_plano_de_saude_canceladas/": "Relatorio_cadop_canceladas.csv",
    }

    FINANCIAL_DIR = "demonstracoes_contabeis/"

    def __init__(self, output_dir: str):
        """
        Inicializa a instância do Scraper e configura a persistência de conexão.

        Estabelece uma sessão HTTP (`requests.Session`) para otimizar conexões TCP (Keep-Alive)
        e define cabeçalhos personalizados (`User-Agent`) para mimetizar um navegador real,
        mitigando riscos de bloqueio (WAF/Anti-bot) por parte do servidor da ANS.

        Args:
            output_dir (str): Caminho absoluto ou relativo do diretório onde os arquivos
                baixados (ZIPs e CSVs brutos) serão armazenados.
        """
        self.output_dir = output_dir
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) python-requests/IntuitiveTest"
            }
        )

    def _get_soup(self, url: str) -> Optional[BeautifulSoup]:
        """
        Executa uma requisição HTTP GET e processa a resposta HTML (Parsing).

        Utiliza a sessão compartilhada (`self.session`) para realizar a chamada de rede
        com timeout de segurança (30s). Em caso de sucesso, converte o texto bruto em um
        objeto DOM navegável. Em caso de erro (4xx/5xx ou timeout), captura a exceção,
        registra no log e retorna None para garantir a continuidade do fluxo (Fail-safe).

        Args:
            url (str): A URL completa do diretório ou página a ser acessada.

        Returns:
            Optional[BeautifulSoup]: Objeto BeautifulSoup instanciado com parser 'html.parser',
            ou None se ocorrer qualquer erro de conexão ou HTTP.
        """
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except requests.RequestException as e:
            logging.error(f"Erro ao acessar {url}: {e}")
            return None

    def _download_file(self, url: str, filename: str) -> Optional[str]:
        """
        Realiza o download de um arquivo remoto utilizando streaming de dados (Chunks).

        Esta função implementa três estratégias de engenharia importantes:
        1. **Eficiência de Memória:** Usa `stream=True` e processa blocos de 8KB, permitindo
           baixar arquivos maiores que a RAM disponível sem travamentos.
        2. **Idempotência (Cache):** Verifica a existência local antes de baixar, economizando
           largura de banda e tempo de execução em reprocessamentos.
        3. **Integridade (Cleanup):** Garante que arquivos parciais/corrompidos sejam
           deletados caso o download seja interrompido por erro.

        Args:
            url (str): Endereço HTTP direto do recurso alvo.
            filename (str): Nome do arquivo para persistência no diretório de saída configurado.

        Returns:
            Optional[str]: O caminho absoluto do arquivo salvo com sucesso, ou None
            caso ocorra erro de rede ou falha de I/O.
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
        """
        Orquestra a extração dos arquivos mestres de cadastro (Ativas e Canceladas).

        Este método é crítico para a etapa de Enriquecimento, pois os arquivos baixados
        aqui (CADOP) funcionam como a "Pedra de Roseta" que traduz o código interno
        da ANS (`REG_ANS`) para identificadores fiscais (`CNPJ`) e jurídicos (`Razão Social`).

        Lógica de Extração:
        1. Itera sobre as fontes configuradas em `self.CADOP_SOURCES`.
        2. Acessa o diretório FTP correspondente.
        3. Realiza uma busca heurística (substring match) nos links da página para encontrar
           o CSV alvo, garantindo resiliência contra pequenas variações de nomeação por parte da ANS.

        Returns:
            List[str]: Lista contendo os caminhos locais (file paths) dos arquivos CSV
            baixados com sucesso.
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
        """
        Executa o fluxo principal de scraping (Crawler) para obtenção dos dados brutos.

        Implementa uma estratégia de navegação hierárquica para identificar e baixar
        os arquivos mais recentes disponíveis no servidor da ANS.

        Fluxo de Execução:
        1. **Dados Cadastrais:** Baixa preventivamente os CSVs de operadoras (Ativas/Canceladas).
        2. **Navegação Temporal:** Lista os diretórios de anos em ordem decrescente (do mais recente para o mais antigo).
        3. **Seleção Heurística:** Itera sobre os arquivos de cada ano buscando padrões de nomes
           que indiquem trimestres (ex: '1T', '3trim') via Regex.
        4. **Critério de Parada:** Interrompe a busca assim que coletar os 3 últimos trimestres
           disponíveis (`MAX_QUARTERS`), otimizando o tempo de execução e o consumo de banda.

        Returns:
            List[str]: Lista consolidada com os caminhos locais (paths) de todos os arquivos
            baixados (Contábeis + Cadastrais), pronta para ser consumida pelo DataProcessor.
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
