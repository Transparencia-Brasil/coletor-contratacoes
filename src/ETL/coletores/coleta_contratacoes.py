"""Coleta contratações do Compras.gov.br em OCDS, filtradas por UF do comprador.

Fonte: publicação 157 do OCP Data Registry (Compras.gov.br / SEGES), com um
arquivo JSON Lines compactado por ano. Cada linha é um processo de contratação
(release compilada OCDS) com as etapas de licitação (`tender`) e adjudicação
(`awards`).

O arquivo do ano inteiro tem centenas de MB e cobre o país todo. O coletor o
baixa para `tmp/`, lê linha a linha sem carregar tudo na memória e mantém apenas
os processos cujo comprador (`buyer`) tem `address.region` igual à UF pedida
(padrão: DF). Opcionalmente, filtra também pela esfera do comprador.

Uso (a partir da raiz do repositório):
    python tasks/coleta-contratacoes/src/coleta_contratacoes.py --anos 2025 2026
    python tasks/coleta-contratacoes/src/coleta_contratacoes.py --anos 2026 --esferas Distrital
    python tasks/coleta-contratacoes/src/coleta_contratacoes.py --anos 2026 --limite 5000   # teste rápido

Saídas em `tasks/coleta-contratacoes/outputs/<alias>/`:
    contratacoes.jsonl.gz  releases OCDS originais, só as selecionadas
    contratacoes.csv       uma linha por processo de contratação
    itens.csv              uma linha por item licitado (tender.items + lote)
    adjudicacoes.csv       uma linha por item adjudicado e fornecedor
    monitoramento.csv      resumo de cada ano processado
    erros.csv              linhas ou anos que falharam
    coleta.log             log da execução
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Iterator

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

TASK_DIR = Path(__file__).resolve().parents[1]
RAIZ_REPO = TASK_DIR.parents[1]

load_dotenv(RAIZ_REPO / ".env")

URL_DOWNLOAD = os.getenv(
    "OCP_COMPRAS_DOWNLOAD_URL",
    "https://data.open-contracting.org/en/publication/157/download",
)
TIMEOUT = int(os.getenv("API_TIMEOUT_SECONDS", "30"))
TAMANHO_BLOCO = 1024 * 1024  # 1 MB
LOG_A_CADA_BYTES = 50 * TAMANHO_BLOCO

log = logging.getLogger("coleta_contratacoes")


# ------------------------------------------------------------------------------
# Colunas dos CSVs
# ------------------------------------------------------------------------------

COLUNAS_CONTRATACOES = [
    "ano_arquivo", "ocid", "release_id", "data_release", "etapa_inicial",
    "comprador_cnpj", "comprador_nome", "comprador_codigo_unidade",
    "comprador_nome_unidade", "comprador_uf", "comprador_municipio",
    "comprador_esfera", "comprador_poder",
    "licitacao_id", "titulo", "objeto", "metodo_contratacao",
    "metodo_contratacao_detalhes", "amparo_legal", "valor_estimado", "moeda",
    "data_inicio_propostas", "data_fim_propostas", "forma_submissao",
    "qtd_itens", "qtd_adjudicacoes", "valor_adjudicado_total",
]

COLUNAS_ITENS = [
    "ocid", "item_id", "lote_id", "descricao", "codigo_catalogo",
    "esquema_classificacao", "uri_catalogo", "categoria_id", "categoria_nome",
    "ncm_nbs", "quantidade", "unidade", "valor_unitario_estimado",
    "valor_total_estimado", "orcamento_sigiloso", "criterio_julgamento",
    "situacao",
]

COLUNAS_ADJUDICACOES = [
    "ocid", "adjudicacao_id", "data_adjudicacao", "item_id", "lote_id",
    "descricao_item", "fornecedor_id", "fornecedor_nome", "quantidade",
    "unidade", "valor_unitario", "valor_total_item", "valor_adjudicacao",
    "situacao_item",
]

COLUNAS_MONITORAMENTO = [
    "ano", "url", "arquivo_bytes", "linhas_lidas", "contratacoes_selecionadas",
    "itens", "adjudicacoes", "erros", "inicio", "fim", "duracao_segundos",
    "status",
]

COLUNAS_ERROS = ["ano", "linha", "ocid", "etapa", "erro"]


# ------------------------------------------------------------------------------
# Utilidades
# ------------------------------------------------------------------------------

def sessao_http() -> requests.Session:
    """Sessão com novas tentativas e espera crescente para falhas transitórias."""
    tentativas = Retry(
        total=8,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        respect_retry_after_header=True,
    )
    sessao = requests.Session()
    sessao.mount("https://", HTTPAdapter(max_retries=tentativas))
    sessao.headers["User-Agent"] = "medicamentos-fdcc/coleta-contratacoes (Transparencia Brasil)"
    return sessao


def sem_prefixo(identificador: str | None) -> str | None:
    """'BR-CNPJ-00394700000108' -> '00394700000108'."""
    if not identificador:
        return identificador
    return identificador.removeprefix("BR-CNPJ-")


def get(obj: Any, *caminho: str, padrao: Any = None) -> Any:
    """Acesso seguro a campos aninhados de dicionários."""
    for chave in caminho:
        if not isinstance(obj, dict):
            return padrao
        obj = obj.get(chave)
        if obj is None:
            return padrao
    return obj


def classificacao(parte: dict, esquema: str) -> str | None:
    """Retorna a descrição da primeira classificação do esquema, ou None."""
    for item in get(parte, "details", "classifications", padrao=[]):
        if item.get("scheme") == esquema:
            return item.get("description")
    return None


def multiplica(a: Any, b: Any) -> float | None:
    """Multiplica valores como floats e arredonda o resultado a quatro casas.

    Retorna None quando a conversão de algum valor falha com TypeError ou
    ValueError.
    """
    try:
        return round(float(a) * float(b), 4)
    except (TypeError, ValueError):
        return None


class EscritorCSV:
    """Abre um CSV com cabeçalho e conta as linhas escritas."""

    def __init__(self, caminho: Path, colunas: list[str]):
        """Abre ou sobrescreve o CSV em UTF-8 e escreve o cabeçalho informado."""
        self.arquivo = caminho.open("w", encoding="utf-8", newline="")
        self.writer = csv.DictWriter(self.arquivo, fieldnames=colunas, extrasaction="raise")
        self.writer.writeheader()
        self.linhas = 0

    def escreve(self, linhas: Iterable[dict]) -> int:
        """Escreve as linhas, atualiza o contador e retorna a quantidade escrita."""
        n = 0
        for linha in linhas:
            self.writer.writerow(linha)
            n += 1
        self.linhas += n
        return n

    def fecha(self) -> None:
        """Fecha o arquivo CSV, descarregando os dados pendentes de escrita."""
        self.arquivo.close()


# ------------------------------------------------------------------------------
# Download
# ------------------------------------------------------------------------------

def baixa_arquivo(sessao: requests.Session, ano: int, destino: Path, forcar: bool) -> tuple[str, int]:
    """Baixa `<ano>.jsonl.gz` para `destino`. Reaproveita o arquivo se já existir."""
    params = {"name": f"{ano}.jsonl.gz"}
    url = requests.Request("GET", URL_DOWNLOAD, params=params).prepare().url

    if destino.exists() and not forcar:
        log.info("Reaproveitando download existente: %s (%.1f MB)", destino, destino.stat().st_size / 1e6)
        return url, destino.stat().st_size

    destino.parent.mkdir(parents=True, exist_ok=True)
    parcial = destino.with_suffix(destino.suffix + ".part")
    log.info("Baixando %s", url)

    with sessao.get(URL_DOWNLOAD, params=params, stream=True, timeout=TIMEOUT) as resposta:
        resposta.raise_for_status()
        total_esperado = int(resposta.headers.get("Content-Length") or 0)
        baixados = 0
        proximo_log = LOG_A_CADA_BYTES
        with parcial.open("wb") as arquivo:
            for bloco in resposta.iter_content(chunk_size=TAMANHO_BLOCO):
                arquivo.write(bloco)
                baixados += len(bloco)
                if baixados >= proximo_log:
                    log.info("  %.0f MB baixados", baixados / 1e6)
                    proximo_log += LOG_A_CADA_BYTES

    if total_esperado and baixados != total_esperado:
        raise IOError(f"Download incompleto: {baixados} de {total_esperado} bytes")

    parcial.replace(destino)
    log.info("Download concluído: %.1f MB", baixados / 1e6)
    return url, baixados


# ------------------------------------------------------------------------------
# Seleção e achatamento
# ------------------------------------------------------------------------------

@dataclass
class Filtro:
    uf: str
    esferas: set[str] = field(default_factory=set)  # vazio = todas

    def comprador(self, release: dict) -> dict | None:
        """Devolve a parte compradora se a release passar no filtro."""
        id_comprador = get(release, "buyer", "id")
        partes = release.get("parties") or []
        comprador = next((p for p in partes if p.get("id") == id_comprador), None)
        if comprador is None:
            comprador = next((p for p in partes if "buyer" in (p.get("roles") or [])), None)
        if comprador is None:
            return None
        if (get(comprador, "address", "region") or "").upper() != self.uf:
            return None
        if self.esferas and classificacao(comprador, "BRA-ESFERA") not in self.esferas:
            return None
        return comprador


def linha_contratacao(release: dict, comprador: dict, ano: int) -> dict:
    """Monta a linha do CSV de contratações com a release, o comprador e o ano.

    Inclui dados da licitação, contagens de itens e adjudicações e a soma dos
    valores adjudicados não nulos, arredondada a duas casas decimais.
    """
    tender = release.get("tender") or {}
    awards = release.get("awards") or []
    unidade = (comprador.get("additionalIdentifiers") or [{}])[0]
    valores_adjudicados = [get(a, "value", "amount") for a in awards]
    valores_adjudicados = [v for v in valores_adjudicados if v is not None]
    return {
        "ano_arquivo": ano,
        "ocid": release.get("ocid"),
        "release_id": release.get("id"),
        "data_release": release.get("date"),
        "etapa_inicial": release.get("initiationType"),
        "comprador_cnpj": get(comprador, "identifier", "id") or sem_prefixo(comprador.get("id")),
        "comprador_nome": comprador.get("name"),
        "comprador_codigo_unidade": unidade.get("id"),
        "comprador_nome_unidade": unidade.get("legalName"),
        "comprador_uf": get(comprador, "address", "region"),
        "comprador_municipio": get(comprador, "address", "locality"),
        "comprador_esfera": classificacao(comprador, "BRA-ESFERA"),
        "comprador_poder": classificacao(comprador, "BRA-PODER"),
        "licitacao_id": tender.get("id"),
        "titulo": tender.get("title"),
        "objeto": tender.get("description"),
        "metodo_contratacao": tender.get("procurementMethod"),
        "metodo_contratacao_detalhes": tender.get("procurementMethodDetails"),
        "amparo_legal": tender.get("procurementMethodRationale"),
        "valor_estimado": get(tender, "value", "amount"),
        "moeda": get(tender, "value", "currency"),
        "data_inicio_propostas": get(tender, "tenderPeriod", "startDate"),
        "data_fim_propostas": get(tender, "tenderPeriod", "endDate"),
        "forma_submissao": ";".join(tender.get("submissionMethod") or []) or None,
        "qtd_itens": len(tender.get("items") or []),
        "qtd_adjudicacoes": len(awards),
        "valor_adjudicado_total": round(sum(valores_adjudicados), 2) if valores_adjudicados else None,
    }


def linhas_itens(release: dict) -> Iterator[dict]:
    """Gera uma linha por item licitado, incluindo dados do lote relacionado."""
    tender = release.get("tender") or {}
    lotes = {lote.get("id"): lote for lote in tender.get("lots") or []}
    for item in tender.get("items") or []:
        lote = lotes.get(item.get("relatedLot"), {})
        valor_unitario = get(item, "unit", "value", "amount")
        yield {
            "ocid": release.get("ocid"),
            "item_id": item.get("id"),
            "lote_id": item.get("relatedLot"),
            "descricao": item.get("description"),
            "codigo_catalogo": get(item, "classification", "id") or lote.get("catalogItemCode"),
            "esquema_classificacao": get(item, "classification", "scheme"),
            "uri_catalogo": get(item, "classification", "uri"),
            "categoria_id": lote.get("catalogItemCategoryId"),
            "categoria_nome": lote.get("catalogItemCategoryName"),
            "ncm_nbs": lote.get("ncmNbsCode"),
            "quantidade": item.get("quantity"),
            "unidade": get(item, "unit", "name"),
            "valor_unitario_estimado": valor_unitario,
            "valor_total_estimado": get(lote, "value", "amount"),
            "orcamento_sigiloso": lote.get("confidentialBudget"),
            "criterio_julgamento": lote.get("awardCriteriaDetails"),
            "situacao": lote.get("statusDetails"),
        }


def linhas_adjudicacoes(release: dict) -> Iterator[dict]:
    """Gera uma linha por combinação de item adjudicado e fornecedor.

    Quando não há fornecedores, gera uma linha por item com seus campos de
    fornecedor nulos. Calcula o valor total do item a partir da quantidade e
    do valor unitário.
    """
    for award in release.get("awards") or []:
        fornecedores = award.get("suppliers") or [{}]
        for item in award.get("items") or []:
            quantidade = item.get("quantity")
            valor_unitario = get(item, "unit", "value", "amount")
            for fornecedor in fornecedores:
                yield {
                    "ocid": release.get("ocid"),
                    "adjudicacao_id": award.get("id"),
                    "data_adjudicacao": award.get("date"),
                    "item_id": item.get("id"),
                    "lote_id": item.get("relatedLot"),
                    "descricao_item": item.get("description"),
                    "fornecedor_id": sem_prefixo(fornecedor.get("id")),
                    "fornecedor_nome": fornecedor.get("name"),
                    "quantidade": quantidade,
                    "unidade": get(item, "unit", "name"),
                    "valor_unitario": valor_unitario,
                    "valor_total_item": multiplica(quantidade, valor_unitario),
                    "valor_adjudicacao": get(award, "value", "amount"),
                    "situacao_item": item.get("statusDetails"),
                }


# ------------------------------------------------------------------------------
# Processamento de um ano
# ------------------------------------------------------------------------------

def processa_ano(
    ano: int,
    arquivo: Path,
    filtro: Filtro,
    saida_jsonl: gzip.GzipFile,
    csvs: dict[str, EscritorCSV],
    limite: int | None,
) -> dict:
    """Lê o JSONL gzip do ano, filtra releases e grava as saídas JSONL e CSV.

    Usa csvs com as chaves contratacoes, itens, adjudicacoes e erros. Registra
    falhas de leitura ou transformação de cada linha e segue o processamento;
    falhas de escrita são propagadas. Um limite positivo restringe o número
    de linhas do arquivo de entrada, incluindo linhas vazias ou descartadas.

    Retorna as contagens de linhas lidas, contratações selecionadas, itens,
    linhas de adjudicação e erros de leitura ou transformação.
    """
    contagem = {"linhas_lidas": 0, "contratacoes_selecionadas": 0, "itens": 0, "adjudicacoes": 0, "erros": 0}

    with gzip.open(arquivo, "rt", encoding="utf-8") as entrada:
        for numero_linha, linha in enumerate(entrada, start=1):
            if limite and numero_linha > limite:
                log.info("Limite de %d linhas atingido.", limite)
                break
            contagem["linhas_lidas"] = numero_linha
            if not linha.strip():
                continue

            ocid = None
            try:
                release = json.loads(linha)
                ocid = release.get("ocid")
                comprador = filtro.comprador(release)
                if comprador is None:
                    continue

                linha_cont = linha_contratacao(release, comprador, ano)
                itens = list(linhas_itens(release))
                adjudicacoes = list(linhas_adjudicacoes(release))
            except Exception as erro:  # noqa: BLE001 — registra e segue
                contagem["erros"] += 1
                csvs["erros"].escreve([{
                    "ano": ano, "linha": numero_linha, "ocid": ocid,
                    "etapa": "leitura", "erro": f"{type(erro).__name__}: {erro}",
                }])
                continue

            saida_jsonl.write(linha if linha.endswith("\n") else linha + "\n")
            csvs["contratacoes"].escreve([linha_cont])
            contagem["itens"] += csvs["itens"].escreve(itens)
            contagem["adjudicacoes"] += csvs["adjudicacoes"].escreve(adjudicacoes)
            contagem["contratacoes_selecionadas"] += 1

            if numero_linha % 50_000 == 0:
                log.info("  %d linhas lidas, %d selecionadas", numero_linha, contagem["contratacoes_selecionadas"])

    return contagem


# ------------------------------------------------------------------------------
# Execução
# ------------------------------------------------------------------------------

def configura_log(pasta: Path) -> None:
    """Configura o logger em INFO com saídas em stdout e pasta/coleta.log."""
    formato = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
    log.setLevel(logging.INFO)
    for handler in (logging.StreamHandler(sys.stdout), logging.FileHandler(pasta / "coleta.log", encoding="utf-8")):
        handler.setFormatter(formato)
        log.addHandler(handler)


def argumentos(argv: list[str] | None = None) -> argparse.Namespace:
    """Interpreta a CLI, valida os anos, normaliza a UF e define o alias.

    Usa os argumentos do processo quando argv é None. Aceita anos de 2021
    até o ano atual e encerra via argparse se os argumentos forem inválidos.
    """
    ano_atual = datetime.now().year
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--anos", type=int, nargs="+", default=[ano_atual],
                        help=f"Anos dos arquivos a baixar (2021 em diante). Padrão: {ano_atual}.")
    parser.add_argument("--uf", default="DF", help="UF do comprador. Padrão: DF.")
    parser.add_argument("--esferas", nargs="*", default=[],
                        help="Esferas do comprador a manter (ex.: Distrital Federal). Padrão: todas.")
    parser.add_argument("--alias", help="Subpasta de saída em outputs/. Padrão: <UF>-<anos>.")
    parser.add_argument("--forcar-download", action="store_true",
                        help="Baixa de novo mesmo que o arquivo já esteja em tmp/.")
    parser.add_argument("--manter-download", action="store_true",
                        help="Não apaga o arquivo baixado em tmp/ ao final.")
    parser.add_argument("--limite", type=int,
                        help="Lê só as N primeiras linhas de cada ano (teste rápido).")
    args = parser.parse_args(argv)

    if any(ano < 2021 or ano > ano_atual for ano in args.anos):
        parser.error(f"--anos deve estar entre 2021 e {ano_atual}")
    args.uf = args.uf.upper()
    args.alias = args.alias or f"{args.uf}-{'-'.join(str(a) for a in sorted(args.anos))}"
    return args


def main(argv: list[str] | None = None) -> int:
    """Executa a coleta dos anos e grava dados, monitoramento, erros e log.

    Continua nos anos seguintes após falhas de download ou processamento.
    Remove downloads de anos concluídos, salvo com --manter-download.
    Retorna 1 se algum ano falhar e 0 caso contrário; erros registrados por
    linha não alteram o código de retorno.
    """
    args = argumentos(argv)

    pasta_saida = TASK_DIR / "outputs" / args.alias
    pasta_tmp = TASK_DIR / "tmp"
    pasta_saida.mkdir(parents=True, exist_ok=True)
    configura_log(pasta_saida)

    filtro = Filtro(uf=args.uf, esferas=set(args.esferas))
    log.info("Coleta de contratações OCDS (Compras.gov.br)")
    log.info("Anos: %s | UF: %s | esferas: %s | saída: %s",
             args.anos, filtro.uf, sorted(filtro.esferas) or "todas", pasta_saida)

    csvs = {
        "contratacoes": EscritorCSV(pasta_saida / "contratacoes.csv", COLUNAS_CONTRATACOES),
        "itens": EscritorCSV(pasta_saida / "itens.csv", COLUNAS_ITENS),
        "adjudicacoes": EscritorCSV(pasta_saida / "adjudicacoes.csv", COLUNAS_ADJUDICACOES),
        "monitoramento": EscritorCSV(pasta_saida / "monitoramento.csv", COLUNAS_MONITORAMENTO),
        "erros": EscritorCSV(pasta_saida / "erros.csv", COLUNAS_ERROS),
    }
    sessao = sessao_http()
    falhou = False

    try:
        with gzip.open(pasta_saida / "contratacoes.jsonl.gz", "wt", encoding="utf-8") as saida_jsonl:
            for ano in sorted(args.anos):
                inicio = datetime.now()
                t0 = time.monotonic()
                arquivo = pasta_tmp / f"{ano}.jsonl.gz"
                monitor = {"ano": ano, "url": None, "arquivo_bytes": None, "inicio": inicio.isoformat(timespec="seconds")}
                log.info("--- Ano %d ---", ano)
                try:
                    monitor["url"], monitor["arquivo_bytes"] = baixa_arquivo(sessao, ano, arquivo, args.forcar_download)
                    contagem = processa_ano(ano, arquivo, filtro, saida_jsonl, csvs, args.limite)
                    monitor.update(contagem, status="ok")
                    log.info("Ano %d: %d linhas lidas, %d contratações, %d itens, %d adjudicações, %d erros",
                             ano, contagem["linhas_lidas"], contagem["contratacoes_selecionadas"],
                             contagem["itens"], contagem["adjudicacoes"], contagem["erros"])
                except Exception as erro:  # noqa: BLE001 — um ano com falha não interrompe os demais
                    falhou = True
                    log.exception("Falha no ano %d", ano)
                    csvs["erros"].escreve([{"ano": ano, "linha": None, "ocid": None,
                                            "etapa": "download/leitura", "erro": f"{type(erro).__name__}: {erro}"}])
                    monitor["status"] = "falha"
                finally:
                    monitor["fim"] = datetime.now().isoformat(timespec="seconds")
                    monitor["duracao_segundos"] = round(time.monotonic() - t0, 1)
                    csvs["monitoramento"].escreve([monitor])
                    if not args.manter_download and arquivo.exists() and monitor["status"] == "ok":
                        arquivo.unlink()
    finally:
        for escritor in csvs.values():
            escritor.fecha()

    log.info("Total: %d contratações, %d itens, %d adjudicações, %d erros",
             csvs["contratacoes"].linhas, csvs["itens"].linhas,
             csvs["adjudicacoes"].linhas, csvs["erros"].linhas)
    log.info("Arquivos em %s", pasta_saida)
    return 1 if falhou else 0


if __name__ == "__main__":
    sys.exit(main())
