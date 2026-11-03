"""Coleta contratações do Compras.gov.br em OCDS, filtradas por UF do comprador.

Fonte: publicação 157 do OCP Data Registry (Compras.gov.br / SEGES), com um
arquivo JSON Lines compactado por ano. Cada linha é um processo de contratação
(release compilada OCDS) com as etapas de licitação (`tender`) e adjudicação
(`awards`).

O arquivo do ano inteiro tem centenas de MB e cobre o país todo. O coletor o
baixa para uma pasta temporária dentro do destino, lê linha a linha sem carregar
tudo na memória e mantém apenas os processos cujo comprador (`buyer`) tem
`address.region` igual à UF pedida (padrão: DF). Opcionalmente, filtra também
pela esfera do comprador. O arquivo baixado é apagado ao final.

Uso, a partir de qualquer diretório:
    python src/ETL/coletores/coleta_contratacoes.py --anos 2025 2026
    python src/ETL/coletores/coleta_contratacoes.py --anos 2026 --esferas Distrital
    python src/ETL/coletores/coleta_contratacoes.py --anos 2026 --limite 5000   # teste rápido
    python src/ETL/coletores/coleta_contratacoes.py --anos 2026 --saida /caminho/da/pasta

Sem --saida, as saídas vão para
outputs/contratacoes/<UF>[-<esferas>]-<anos>/<AAAAMMDD_HHMMSS>/, na raiz do
repositório. Uma pasta de destino que já tenha arquivos é recusada.

Arquivos gerados no destino:
    contratacoes.jsonl.gz  releases OCDS originais, só as selecionadas
    contratacoes.csv       uma linha por processo de contratação
    itens.csv              uma linha por item licitado (tender.items + lote)
    adjudicacoes.csv       uma linha por item adjudicado e fornecedor
    monitoramento.csv      resumo de cada ano processado
    erros.csv              linhas ou anos que falharam
    coleta.log             log da execução

Códigos de saída: 0 sucesso; 1 algum ano falhou; 2 argumento inválido ou
destino já ocupado.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import logging
import os
import re
import sys
import tempfile
import time
from contextlib import ExitStack
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Iterator

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

RAIZ_REPO = Path(__file__).resolve().parents[3]
PASTA_SAIDA_PADRAO = RAIZ_REPO / "outputs" / "contratacoes"

load_dotenv(RAIZ_REPO / ".env", override=False)

URL_DOWNLOAD = os.getenv(
    "OCP_COMPRAS_DOWNLOAD_URL",
    "https://data.open-contracting.org/en/publication/157/download",
)
TIMEOUT = float(os.getenv("DOWNLOAD_TIMEOUT_SECONDS", "60"))
TAMANHO_BLOCO = 1024 * 1024  # 1 MiB
LOG_A_CADA_BYTES = 50 * TAMANHO_BLOCO
LOG_A_CADA_LINHAS = 50_000

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

# Nome de cada CSV gerado (sem extensão) e suas colunas.
CSVS = {
    "contratacoes": COLUNAS_CONTRATACOES,
    "itens": COLUNAS_ITENS,
    "adjudicacoes": COLUNAS_ADJUDICACOES,
    "monitoramento": COLUNAS_MONITORAMENTO,
    "erros": COLUNAS_ERROS,
}


# ------------------------------------------------------------------------------
# Utilidades
# ------------------------------------------------------------------------------

def sessao_http() -> requests.Session:
    """Cria uma sessão que repete GETs com falhas transitórias.

    Repete até 8 vezes falhas de conexão e respostas 429, 500, 502, 503 e 504,
    com espera crescente de fator 2 e respeito ao cabeçalho Retry-After.
    Esgotadas as tentativas, a última resposta é devolvida para quem chamou
    tratar o status.
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
    """CSV com cabeçalho que conta as linhas escritas; usado como context manager."""

    def __init__(self, caminho: Path, colunas: list[str]):
        """Cria o CSV em UTF-8 e escreve o cabeçalho informado."""
        self.arquivo = caminho.open("w", encoding="utf-8", newline="")
        self.writer = csv.DictWriter(self.arquivo, fieldnames=colunas, extrasaction="raise")
        self.writer.writeheader()
        self.linhas = 0

    def escreve(self, linhas: Iterable[dict]) -> None:
        """Escreve as linhas e atualiza o contador."""
        linhas = list(linhas)
        self.writer.writerows(linhas)
        self.linhas += len(linhas)

    def __enter__(self) -> EscritorCSV:
        return self

    def __exit__(self, *_: object) -> None:
        self.arquivo.close()


# ------------------------------------------------------------------------------
# Download
# ------------------------------------------------------------------------------

def baixa_arquivo(sessao: requests.Session, ano: int, destino: Path) -> tuple[str, int]:
    """Baixa `<ano>.jsonl.gz` em streaming para `destino`.

    Returns:
        URL baixada e total de bytes gravados.

    Raises:
        requests.HTTPError: Se a resposta final não for 2xx.
        IOError: Se o total recebido diferir do Content-Length declarado.
    """
    params = {"name": f"{ano}.jsonl.gz"}
    url = requests.Request("GET", URL_DOWNLOAD, params=params).prepare().url
    log.info("Baixando %s", url)

    with sessao.get(URL_DOWNLOAD, params=params, stream=True, timeout=TIMEOUT) as resposta:
        resposta.raise_for_status()
        total_esperado = int(resposta.headers.get("Content-Length") or 0)
        baixados = 0
        proximo_log = LOG_A_CADA_BYTES
        with destino.open("wb") as arquivo:
            for bloco in resposta.iter_content(chunk_size=TAMANHO_BLOCO):
                arquivo.write(bloco)
                baixados += len(bloco)
                if baixados >= proximo_log:
                    log.info("  %.0f MB baixados", baixados / 1e6)
                    proximo_log += LOG_A_CADA_BYTES

    if total_esperado and baixados != total_esperado:
        raise IOError(f"Download incompleto: {baixados} de {total_esperado} bytes")

    log.info("Download concluído: %.1f MB", baixados / 1e6)
    return url, baixados


# ------------------------------------------------------------------------------
# Seleção e achatamento
# ------------------------------------------------------------------------------

@dataclass
class Filtro:
    uf: str
    esferas: set[str] = field(default_factory=set)  # vazio = todas

    def __post_init__(self) -> None:
        # A UF entre aspas, sem diferenciar maiúsculas, como em comprador().
        self._marcador = re.compile(f'"{re.escape(self.uf)}"', re.IGNORECASE)

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

    def pode_passar(self, linha: str) -> bool:
        """Pré-filtro textual, bem mais barato que o json.loads.

        Só descarta linhas em que a UF nem aparece entre aspas; elas falhariam
        em comprador() de qualquer forma. As demais seguem para o filtro real.
        Em 2026, só 6% das linhas do arquivo nacional passam para o DF.
        """
        return self._marcador.search(linha) is not None


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
            "valor_unitario_estimado": get(item, "unit", "value", "amount"),
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
    saida_jsonl: Any,
    csvs: dict[str, EscritorCSV],
    limite: int | None,
) -> dict:
    """Lê o JSONL gzip do ano, filtra releases e grava as saídas JSONL e CSV.

    Usa csvs com as chaves contratacoes, itens, adjudicacoes e erros. Registra
    falhas de leitura ou transformação de cada linha e segue o processamento;
    falhas de escrita são propagadas. Um limite restringe o número de linhas
    do arquivo de entrada, incluindo linhas vazias ou descartadas.

    Returns:
        Contagens de linhas lidas, contratações selecionadas, itens, linhas de
        adjudicação e erros de leitura ou transformação deste ano.
    """
    antes = {nome: escritor.linhas for nome, escritor in csvs.items()}
    linhas_lidas = 0

    with gzip.open(arquivo, "rt", encoding="utf-8") as entrada:
        for numero_linha, linha in enumerate(entrada, start=1):
            if limite and numero_linha > limite:
                log.info("Limite de %d linhas atingido.", limite)
                break
            linhas_lidas = numero_linha
            if numero_linha % LOG_A_CADA_LINHAS == 0:
                log.info("  %d linhas lidas, %d selecionadas",
                         numero_linha, csvs["contratacoes"].linhas - antes["contratacoes"])
            if not filtro.pode_passar(linha):
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
                csvs["erros"].escreve([{
                    "ano": ano, "linha": numero_linha, "ocid": ocid,
                    "etapa": "leitura", "erro": f"{type(erro).__name__}: {erro}",
                }])
                continue

            saida_jsonl.write(linha if linha.endswith("\n") else linha + "\n")
            csvs["contratacoes"].escreve([linha_cont])
            csvs["itens"].escreve(itens)
            csvs["adjudicacoes"].escreve(adjudicacoes)

    def novas(nome: str) -> int:
        return csvs[nome].linhas - antes[nome]

    return {
        "linhas_lidas": linhas_lidas,
        "contratacoes_selecionadas": novas("contratacoes"),
        "itens": novas("itens"),
        "adjudicacoes": novas("adjudicacoes"),
        "erros": novas("erros"),
    }


# ------------------------------------------------------------------------------
# Execução
# ------------------------------------------------------------------------------

def configura_log(pasta: Path) -> None:
    """Configura o logger em INFO com saídas em stdout e pasta/coleta.log."""
    encerra_log()
    formato = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
    log.setLevel(logging.INFO)
    for handler in (logging.StreamHandler(sys.stdout), logging.FileHandler(pasta / "coleta.log", encoding="utf-8")):
        handler.setFormatter(formato)
        log.addHandler(handler)


def encerra_log() -> None:
    """Fecha e remove os handlers do logger, liberando o coleta.log."""
    for handler in list(log.handlers):
        handler.close()
        log.removeHandler(handler)


def inteiro_positivo(texto: str) -> int:
    """Converte para int maior que zero; usado como type do argparse."""
    try:
        valor = int(texto)
    except ValueError:
        valor = 0
    if valor <= 0:
        raise argparse.ArgumentTypeError(f"valor inválido: {texto!r}; use um inteiro positivo")
    return valor


def argumentos(argv: list[str] | None = None) -> argparse.Namespace:
    """Interpreta a CLI, valida os anos, normaliza a UF e define a pasta de saída.

    Usa os argumentos do processo quando argv é None. Aceita anos de 2021
    até o ano atual e encerra via argparse (código 2) se os argumentos forem
    inválidos.
    """
    ano_atual = datetime.now().year
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--anos", type=int, nargs="+", default=[ano_atual],
                        help=f"Anos dos arquivos a baixar (2021 em diante). Padrão: {ano_atual}.")
    parser.add_argument("--uf", default="DF", help="UF do comprador. Padrão: DF.")
    parser.add_argument("--esferas", nargs="*", default=[],
                        help="Esferas do comprador a manter (ex.: Distrital Federal). Padrão: todas.")
    parser.add_argument("--saida", type=Path,
                        help="Pasta de destino, que não pode ter arquivos. Caminho relativo é resolvido "
                             "a partir do diretório atual. Padrão: outputs/contratacoes/<UF>-<anos>/<data_hora>/.")
    parser.add_argument("--limite", type=inteiro_positivo,
                        help="Lê só as N primeiras linhas de cada ano (teste rápido).")
    args = parser.parse_args(argv)

    if any(ano < 2021 or ano > ano_atual for ano in args.anos):
        parser.error(f"--anos deve estar entre 2021 e {ano_atual}")
    args.anos = sorted(set(args.anos))
    args.uf = args.uf.strip().upper()
    if not args.uf:
        parser.error("--uf não pode ser vazio")
    if args.saida is None:
        alias = "-".join([args.uf, *sorted(set(args.esferas)), *map(str, args.anos)]).replace(" ", "_")
        args.saida = PASTA_SAIDA_PADRAO / alias / f"{datetime.now():%Y%m%d_%H%M%S}"
    return args


def main(argv: list[str] | None = None) -> int:
    """Executa a coleta dos anos e grava dados, monitoramento, erros e log.

    Continua nos anos seguintes após falhas de download ou processamento. O
    arquivo baixado de cada ano é apagado ao fim do processamento.

    Returns:
        0 se todos os anos deram certo; 1 se algum ano falhou; 2 se a pasta de
        destino já tem arquivos. Erros registrados por linha não alteram o
        código de retorno.
    """
    args = argumentos(argv)
    pasta_saida: Path = args.saida.resolve()

    if pasta_saida.exists() and (not pasta_saida.is_dir() or any(pasta_saida.iterdir())):
        print(f"Erro: o destino '{pasta_saida}' já existe e não é uma pasta vazia.", file=sys.stderr)
        return 2
    pasta_saida.mkdir(parents=True, exist_ok=True)
    configura_log(pasta_saida)

    try:
        filtro = Filtro(uf=args.uf, esferas=set(args.esferas))
        log.info("Coleta de contratações OCDS (Compras.gov.br)")
        log.info("Anos: %s | UF: %s | esferas: %s | saída: %s",
                 args.anos, filtro.uf, sorted(filtro.esferas) or "todas", pasta_saida)
        falhou = False

        with ExitStack() as pilha:
            csvs = {
                nome: pilha.enter_context(EscritorCSV(pasta_saida / f"{nome}.csv", colunas))
                for nome, colunas in CSVS.items()
            }
            saida_jsonl = pilha.enter_context(
                gzip.open(pasta_saida / "contratacoes.jsonl.gz", "wt", encoding="utf-8"))
            pasta_tmp = Path(pilha.enter_context(
                tempfile.TemporaryDirectory(prefix=".download-", dir=pasta_saida)))
            sessao = pilha.enter_context(sessao_http())

            for ano in args.anos:
                log.info("--- Ano %d ---", ano)
                t0 = time.monotonic()
                monitor = {"ano": ano, "inicio": datetime.now().isoformat(timespec="seconds")}
                arquivo = pasta_tmp / f"{ano}.jsonl.gz"
                try:
                    monitor["url"], monitor["arquivo_bytes"] = baixa_arquivo(sessao, ano, arquivo)
                    contagem = processa_ano(ano, arquivo, filtro, saida_jsonl, csvs, args.limite)
                    monitor.update(contagem, status="ok")
                    log.info("Ano %d: %d linhas lidas, %d contratações, %d itens, %d adjudicações, %d erros",
                             ano, *contagem.values())
                except Exception as erro:  # noqa: BLE001 — um ano com falha não interrompe os demais
                    falhou = True
                    log.exception("Falha no ano %d", ano)
                    csvs["erros"].escreve([{"ano": ano, "linha": None, "ocid": None,
                                            "etapa": "download/leitura", "erro": f"{type(erro).__name__}: {erro}"}])
                    monitor["status"] = "falha"
                finally:
                    arquivo.unlink(missing_ok=True)
                    monitor["fim"] = datetime.now().isoformat(timespec="seconds")
                    monitor["duracao_segundos"] = round(time.monotonic() - t0, 1)
                    csvs["monitoramento"].escreve([monitor])

        log.info("Total: %d contratações, %d itens, %d adjudicações, %d erros",
                 csvs["contratacoes"].linhas, csvs["itens"].linhas,
                 csvs["adjudicacoes"].linhas, csvs["erros"].linhas)
        log.info("Arquivos em %s", pasta_saida)
        return 1 if falhou else 0
    finally:
        encerra_log()


if __name__ == "__main__":
    # No Windows, a saída redirecionada (pipe, tee, arquivo) usaria cp1252; os logs
    # de quem chama o coletor devem sair em UTF-8.
    for fluxo in (sys.stdout, sys.stderr):
        fluxo.reconfigure(encoding="utf-8")
    sys.exit(main())
