"""Baixa o arquivo anual de contratações de medicamentos em OCDS e verifica a integridade.

Fonte: publicação 153 do OCP Data Registry (Medicamentos Transparentes), com um
arquivo JSON Lines compactado por ano, `{ano}.jsonl.gz`, em que cada linha é um
processo de contratação (release compilada OCDS). O arquivo é salvo byte a byte
como foi recebido: nenhuma linha é filtrada, convertida ou reescrita.

Uso, a partir de qualquer diretório:
    python src/ETL/coletores/coleta_contratacoes.py --ano 2026
    python src/ETL/coletores/coleta_contratacoes.py --ano 2026 --saida caminho/2026.jsonl.gz

Sem --saida, o arquivo vai para
outputs/contratacoes/{ano}/{AAAAMMDD}_{HHMMSS}_contratacoes_{ano}.jsonl.gz, na
raiz do repositório. Um destino que já exista é recusado.

O download é gravado num .part ao lado do destino. Antes de virar o arquivo
final, o total recebido é conferido com o Content-Length e o gzip é lido
inteiro, o que confere o CRC e o tamanho original gravados nele.

Códigos de saída: 0 arquivo salvo e íntegro; 1 falha de rede, HTTP, download
incompleto, gzip inválido ou erro de escrita; 2 argumento ou configuração
inválidos, ano não publicado ou destino já existente.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import logging
import math
import os
import re
import sys
import time
import zlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
import urllib3.exceptions
from urllib3.util.retry import Retry

RAIZ_REPO = Path(__file__).resolve().parents[3]
PASTA_SAIDA = RAIZ_REPO / "outputs" / "contratacoes"
URL_PADRAO = "https://data.open-contracting.org/en/publication/153/download"
TIMEOUT_PADRAO = "60"
TAMANHO_BLOCO = 1024 * 1024  # 1 MiB
# [0-9] em vez de \d, que também aceitaria dígitos Unicode como "２０２６".
PADRAO_ANO = re.compile(r"[0-9]{4}")

log = logging.getLogger("coleta_contratacoes")


# ------------------------------------------------------------------------------
# Configuração e argumentos
# ------------------------------------------------------------------------------

@dataclass(frozen=True)
class Configuracao:
    """Parâmetros de acesso à fonte.

    Attributes:
        url_base: Endereço de download da publicação, sem a query do ano.
        timeout: Segundos de espera, por tentativa, para conectar e entre
            leituras sucessivas da resposta.
    """

    url_base: str
    timeout: float


def configuracao() -> Configuracao:
    """Lê a configuração do ambiente e do .env da raiz do repositório.

    Variáveis já definidas no ambiente têm precedência sobre o .env, e uma
    variável definida mas vazia usa o padrão.

    Raises:
        ValueError: Se a URL não for HTTP(S) com host, ou tiver query ou
            fragmento, ou se o timeout não for um número positivo e finito.
    """
    load_dotenv(RAIZ_REPO / ".env", override=False)
    url_base = (os.getenv("OCP_DOWNLOAD_URL") or URL_PADRAO).rstrip("/")
    url = urlsplit(url_base)
    if url.scheme not in ("https", "http") or not url.netloc or url.query or url.fragment:
        raise ValueError("OCP_DOWNLOAD_URL deve ser uma URL HTTP(S) sem query ou fragmento")
    try:
        timeout = float(os.getenv("DOWNLOAD_TIMEOUT_SECONDS") or TIMEOUT_PADRAO)
    except ValueError as erro:
        raise ValueError("DOWNLOAD_TIMEOUT_SECONDS deve ser um número") from erro
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("DOWNLOAD_TIMEOUT_SECONDS deve ser positivo e finito")
    return Configuracao(url_base, timeout)


def valida_ano(texto: str) -> str:
    """Aceita somente quatro dígitos ASCII; usado como type do argparse.

    Se o ano tem arquivo publicado não é verificado aqui: quem decide é a
    fonte, que responde 404.
    """
    if not PADRAO_ANO.fullmatch(texto):
        raise argparse.ArgumentTypeError(f"ano inválido: {texto!r}; use quatro dígitos, como 2026")
    return texto


def le_argumentos(argv: list[str] | None = None) -> tuple[str, Path | None]:
    """Lê --ano e --saida da linha de comando; None usa sys.argv.

    Returns:
        O ano e o caminho de --saida, ou None se não informado.

    Raises:
        SystemExit: Com código 2, pelo argparse, para argumentos inválidos.
    """
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--ano", required=True, type=valida_ano, help="Ano com quatro dígitos (ex.: 2026).")
    parser.add_argument("--saida", type=Path,
                        help="Caminho completo do arquivo a gravar, relativo ao diretório de execução se "
                             "não for absoluto; sem ele, o arquivo vai para outputs/contratacoes/{ano}/.")
    args = parser.parse_args(argv)
    return args.ano, args.saida


def url_do_ano(config: Configuracao, ano: str) -> str:
    """URL do arquivo do ano, como {url_base}?name=2026.jsonl.gz."""
    return f"{config.url_base}?name={ano}.jsonl.gz"


def destino_padrao(ano: str, momento: datetime) -> Path:
    """Caminho do arquivo quando --saida não é informado.

    A data e a hora vêm primeiro, no formato AAAAMMDD_HHMMSS, para que a ordem
    alfabética seja a cronológica. Exemplo:
    outputs/contratacoes/2026/20261104_153000_contratacoes_2026.jsonl.gz.
    """
    return PASTA_SAIDA / ano / f"{momento:%Y%m%d_%H%M%S}_contratacoes_{ano}.jsonl.gz"


# ------------------------------------------------------------------------------
# Download e verificação
# ------------------------------------------------------------------------------

class AnoNaoPublicado(Exception):
    """A fonte respondeu 404 para o arquivo do ano pedido."""


class DownloadIncompleto(Exception):
    """O total recebido difere do Content-Length declarado pela fonte."""


class GzipInvalido(Exception):
    """O arquivo recebido está vazio, corrompido ou não é gzip."""


@dataclass(frozen=True)
class Download:
    """Resumo de um download salvo e verificado.

    Attributes:
        url_final: URL que entregou o arquivo, depois dos redirecionamentos.
            Na OCP, traz o número da versão da publicação.
        bytes_gravados: Tamanho do arquivo salvo.
        content_length: Tamanho declarado pela fonte, se enviado.
        last_modified: Cabeçalho Last-Modified da fonte, se enviado.
        sha256: Hash SHA-256 do arquivo salvo, em hexadecimal.
        bytes_descompactados: Tamanho do conteúdo descompactado.
        segundos_download: Duração do download.
        segundos_verificacao: Duração da leitura do gzip.
    """

    url_final: str
    bytes_gravados: int
    content_length: int | None
    last_modified: str | None
    sha256: str
    bytes_descompactados: int
    segundos_download: float
    segundos_verificacao: float


def sessao_http() -> requests.Session:
    """Cria uma sessão que repete GETs com falhas transitórias.

    Repete até 8 vezes falhas de conexão e respostas 429, 500, 502, 503 e 504,
    com espera crescente de fator 2 e respeito ao cabeçalho Retry-After.
    Esgotadas as tentativas, a última resposta é devolvida para quem chamou
    tratar o status. Uma falha no meio da leitura do corpo, depois que a
    resposta começou a chegar, não é repetida.
    """
    tentativas = Retry(
        total=8,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    sessao = requests.Session()
    adaptador = HTTPAdapter(max_retries=tentativas)
    sessao.mount("https://", adaptador)
    sessao.mount("http://", adaptador)
    sessao.headers["User-Agent"] = "Transparencia-Brasil/coletor-contratacoes"
    # Sem compressão de transporte: o arquivo já é .gz e precisa chegar como foi
    # publicado para conferir com o Content-Length.
    sessao.headers["Accept-Encoding"] = "identity"
    return sessao


def verifica_gzip(arquivo: Path) -> int:
    """Lê o gzip inteiro, descartando o conteúdo, para conferir a integridade.

    O módulo gzip confere, ao fim de cada membro, o CRC32 e o tamanho original
    gravados no arquivo. Nada do conteúdo é interpretado.

    Returns:
        Total de bytes descompactados.

    Raises:
        GzipInvalido: Se o arquivo estiver vazio, cortado, corrompido, tiver
            dados que não são gzip depois do fim ou não for gzip.
    """
    # Um arquivo vazio é lido pelo gzip como conteúdo vazio, sem erro.
    if arquivo.stat().st_size == 0:
        raise GzipInvalido("o arquivo recebido está vazio")
    total = 0
    try:
        with gzip.open(arquivo, "rb") as entrada:
            while bloco := entrada.read(TAMANHO_BLOCO):
                total += len(bloco)
    except (OSError, EOFError, zlib.error) as erro:  # BadGzipFile é um OSError
        raise GzipInvalido(f"{type(erro).__name__}: {erro}") from erro
    return total


def baixa_e_verifica(sessao: requests.Session, url: str, destino: Path, timeout: float) -> Download:
    """Baixa a URL, verifica a integridade e só então publica o arquivo no destino.

    O corpo é gravado em blocos num .part ao lado do destino, byte a byte como
    chegou (um Content-Encoding não é decodificado), com o SHA-256 calculado
    durante a gravação. Depois de conferido o Content-Length e lido
    o gzip inteiro, o .part substitui o destino de forma atômica. Em qualquer
    falha ou interrupção, o .part é removido e o destino não é criado.

    Args:
        sessao: Sessão HTTP, normalmente criada por sessao_http().
        url: Endereço do arquivo.
        destino: Caminho final; a pasta é criada se não existir.
        timeout: Segundos para conectar e entre leituras sucessivas da resposta.

    Raises:
        AnoNaoPublicado: Se a fonte responder 404.
        requests.HTTPError: Para outro status diferente de 200.
        requests.RequestException: Para falhas de conexão ou de leitura.
        DownloadIncompleto: Se o total gravado diferir do Content-Length.
        GzipInvalido: Se o arquivo não passar na leitura do gzip.
        OSError: Para falhas ao criar a pasta ou gravar o arquivo.
    """
    destino.parent.mkdir(parents=True, exist_ok=True)
    part = destino.with_name(destino.name + ".part")
    try:
        inicio = time.monotonic()
        with sessao.get(url, stream=True, timeout=(timeout, timeout)) as resposta:
            if resposta.status_code == 404:
                raise AnoNaoPublicado(url)
            if resposta.status_code != 200:
                raise requests.HTTPError(f"HTTP {resposta.status_code} em {url}", response=resposta)
            cabecalho_tamanho = resposta.headers.get("Content-Length")
            content_length = int(cabecalho_tamanho) if cabecalho_tamanho is not None else None
            hash_arquivo = hashlib.sha256()
            gravados = 0
            with part.open("wb") as saida:
                try:
                    # Bytes crus da conexão: o iter_content decodificaria um
                    # Content-Encoding, e o arquivo deixaria de ser o publicado.
                    for bloco in resposta.raw.stream(TAMANHO_BLOCO, decode_content=False):
                        saida.write(bloco)
                        hash_arquivo.update(bloco)
                        gravados += len(bloco)
                except urllib3.exceptions.HTTPError as erro:
                    # Corpo cortado ou timeout de leitura, com o mesmo tipo das
                    # demais falhas de rede.
                    raise requests.ConnectionError(f"{type(erro).__name__}: {erro}") from erro
            if content_length is not None and gravados != content_length:
                raise DownloadIncompleto(f"recebidos {gravados} de {content_length} bytes declarados em {url}")
            url_final = resposta.url
            last_modified = resposta.headers.get("Last-Modified")
        segundos_download = time.monotonic() - inicio

        inicio = time.monotonic()
        descompactados = verifica_gzip(part)
        segundos_verificacao = time.monotonic() - inicio
        part.replace(destino)
    finally:
        part.unlink(missing_ok=True)
    return Download(url_final, gravados, content_length, last_modified, hash_arquivo.hexdigest(),
                    descompactados, segundos_download, segundos_verificacao)


# ------------------------------------------------------------------------------
# Execução
# ------------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    """Baixa e verifica o arquivo do ano pedido e devolve o código de saída.

    Returns:
        0 quando o arquivo é salvo e íntegro; 1 para falha de rede, HTTP
        diferente de 200 e de 404, download incompleto, gzip inválido ou erro
        de escrita; 2 para configuração inválida, ano não publicado ou destino
        já existente. Argumentos inválidos encerram pelo argparse, também com
        código 2.
    """
    ano, saida = le_argumentos(argv)
    try:
        config = configuracao()
    except ValueError as erro:
        log.error("Configuração inválida: %s", erro)
        return 2

    url = url_do_ano(config, ano)
    destino = saida.resolve() if saida is not None else destino_padrao(ano, datetime.now())
    if destino.exists():
        log.error("O destino %s já existe; informe um caminho ainda não usado", destino)
        return 2

    log.info("Baixando %s para %s", url, destino)
    try:
        with sessao_http() as sessao:
            download = baixa_e_verifica(sessao, url, destino, config.timeout)
    except AnoNaoPublicado:
        log.error("Ano %s não publicado na fonte (HTTP 404 em %s)", ano, url)
        return 2
    except GzipInvalido as erro:
        log.error("O arquivo recebido não é um gzip íntegro: %s", erro)
        return 1
    except (requests.RequestException, DownloadIncompleto, OSError) as erro:
        log.error("Falha no download: %s", erro)
        return 1

    log.info(
        "Arquivo salvo em %s | URL final: %s | %d bytes (Content-Length: %s) | Last-Modified: %s | "
        "SHA-256: %s | %d bytes descompactados | download %.1f s, verificação %.1f s",
        destino,
        download.url_final,
        download.bytes_gravados,
        download.content_length if download.content_length is not None else "não informado",
        download.last_modified or "não informado",
        download.sha256,
        download.bytes_descompactados,
        download.segundos_download,
        download.segundos_verificacao,
    )
    return 0


if __name__ == "__main__":
    # No Windows, a saída redirecionada (pipe, tee, arquivo) usaria cp1252; os logs
    # de quem chama o coletor devem sair em UTF-8.
    for fluxo in (sys.stdout, sys.stderr):
        fluxo.reconfigure(encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    sys.exit(main())
