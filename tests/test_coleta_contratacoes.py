"""Testes do coletor de contratações, sem acesso à OCP."""

import csv
import gzip
import json
import os
import shutil
import subprocess
import tempfile
import threading
import unittest
from contextlib import nullcontext
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from src.ETL.coletores import coleta_contratacoes as coletor

RAIZ = Path(__file__).resolve().parents[1]
WRAPPER = RAIZ / "src" / "ETL" / "coletores" / "run-coleta-contratacoes.sh"
ANO = datetime.now().year


def parte(id_, uf, esfera="Distrital", papeis=("buyer",)):
    """Parte OCDS com endereço e classificação de esfera e poder."""
    return {
        "id": id_,
        "name": f"Órgão {id_}",
        "roles": list(papeis),
        "identifier": {"id": id_.removeprefix("BR-CNPJ-")},
        "additionalIdentifiers": [{"id": "926001", "legalName": "Unidade X"}],
        "address": {"region": uf, "locality": "Brasília"},
        "details": {"classifications": [
            {"scheme": "BRA-ESFERA", "description": esfera},
            {"scheme": "BRA-PODER", "description": "Executivo"},
        ]},
    }


def release(ocid, uf="DF", esfera="Distrital", com_buyer=True):
    """Release compilada com 2 itens em 1 lote e 1 adjudicação com 2 fornecedores."""
    comprador = parte("BR-CNPJ-00394700000108", uf, esfera)
    fornecedor = parte("BR-CNPJ-11111111000111", "SP", "Não se Aplica", papeis=("supplier",))
    return {
        "ocid": ocid,
        "id": f"{ocid}-1",
        "date": "2026-03-01T10:00:00Z",
        "initiationType": "tender",
        "buyer": {"id": comprador["id"]} if com_buyer else {},
        "parties": [fornecedor, comprador],
        "tender": {
            "id": "T1",
            "title": "Pregão 1/2026",
            "description": "Aquisição de medicamentos",
            "procurementMethod": "open",
            "procurementMethodDetails": "Pregão - Eletrônico",
            "procurementMethodRationale": "Lei 14.133/2021, Art. 28, I",
            "value": {"amount": 1000.5, "currency": "BRL"},
            "tenderPeriod": {"startDate": "2026-03-02", "endDate": "2026-03-20"},
            "submissionMethod": ["electronicSubmission"],
            "lots": [{
                "id": "L1", "catalogItemCode": "999", "catalogItemCategoryId": 7,
                "catalogItemCategoryName": "Medicamentos", "ncmNbsCode": "3004",
                "value": {"amount": 1000.5}, "confidentialBudget": False,
                "awardCriteriaDetails": "Menor preço", "statusDetails": "Homologado",
            }],
            "items": [
                {"id": "1", "relatedLot": "L1", "description": "Dipirona",
                 "classification": {"id": "267502", "scheme": "CATMAT", "uri": "https://x/267502"},
                 "quantity": 10, "unit": {"name": "Comprimido", "value": {"amount": 0.5}}},
                {"id": "2", "relatedLot": "L1", "description": "Paracetamol",
                 "quantity": 3, "unit": {"name": "Frasco"}},
            ],
        },
        "awards": [{
            "id": "A1", "date": "2026-03-25",
            "value": {"amount": 800.123},
            "suppliers": [{"id": "BR-CNPJ-11111111000111", "name": "Fornecedor 1"},
                          {"id": "BR-CNPJ-22222222000122", "name": "Fornecedor 2"}],
            "items": [{"id": "1", "relatedLot": "L1", "description": "Dipirona", "quantity": 10,
                       "unit": {"name": "Comprimido", "value": {"amount": 0.333}},
                       "statusDetails": "Adjudicado"}],
        }],
    }


def jsonl_gz(*linhas):
    """Compacta as linhas (dicts viram JSON; strings vão como estão) num .jsonl.gz."""
    texto = "".join((json.dumps(l, ensure_ascii=False) if isinstance(l, dict) else l) + "\n" for l in linhas)
    return gzip.compress(texto.encode("utf-8"))


def release_sp_com_fornecedor_df(ocid):
    """Comprador em SP e fornecedor no DF: passa no pré-filtro, mas não no filtro."""
    r = release(ocid, "SP", "Estadual")
    r["parties"][0]["address"]["region"] = "DF"
    return r


# Arquivo de um ano: 2 processos no DF (um distrital, um federal), 1 em SP, 1 em
# SP com fornecedor no DF, 1 linha inválida que cita "DF", 1 linha vazia e 1
# processo do DF em minúsculas sem buyer.id (comprador achado pelo papel).
LINHAS = [
    release("ocds-1", "DF", "Distrital"),
    release("ocds-2", "DF", "Federal"),
    release("ocds-3", "SP", "Estadual"),
    release_sp_com_fornecedor_df("ocds-4"),
    '{"ocid": "ocds-quebrado", "region": "DF"',
    "",
    release("ocds-5", "df", "Distrital", com_buyer=False),
]
CORPO = jsonl_gz(*LINHAS)


def le_csv(caminho):
    with open(caminho, encoding="utf-8", newline="") as arquivo:
        return list(csv.DictReader(arquivo))


class ServidorLocal:
    """Servidor HTTP em thread que serve sempre o mesmo corpo, ou um status de erro."""

    def __init__(self):
        self.status = 200
        self.corpo = CORPO
        self.content_length = None  # None: tamanho real
        self.caminhos = []
        self.cabecalhos_recebidos = []
        servidor = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                servidor.caminhos.append(self.path)
                servidor.cabecalhos_recebidos.append(dict(self.headers))
                self.send_response(servidor.status)
                tamanho = servidor.content_length or len(servidor.corpo)
                self.send_header("Content-Length", str(tamanho))
                self.end_headers()
                self.wfile.write(servidor.corpo)

            def log_message(self, *args):
                pass

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}/download"
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def fecha(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join()


class TestAchatamento(unittest.TestCase):
    def test_filtro_por_uf_e_esfera(self):
        self.assertIsNotNone(coletor.Filtro("DF").comprador(release("x", "DF")))
        self.assertIsNone(coletor.Filtro("DF").comprador(release("x", "SP")))
        self.assertIsNotNone(coletor.Filtro("DF", {"Distrital"}).comprador(release("x", "DF", "Distrital")))
        self.assertIsNone(coletor.Filtro("DF", {"Distrital"}).comprador(release("x", "DF", "Federal")))

    def test_filtro_acha_comprador_pelo_papel_sem_buyer_id(self):
        comprador = coletor.Filtro("DF").comprador(release("x", "DF", com_buyer=False))
        self.assertEqual(comprador["id"], "BR-CNPJ-00394700000108")

    def test_pre_filtro(self):
        filtro = coletor.Filtro("DF")
        self.assertTrue(filtro.pode_passar(json.dumps(release("x", "df"))))
        self.assertFalse(filtro.pode_passar(json.dumps(release("x", "SP"))))
        self.assertFalse(filtro.pode_passar('{"descricao": "PDF"}'))

    def test_linha_contratacao(self):
        r = release("ocds-1")
        linha = coletor.linha_contratacao(r, coletor.Filtro("DF").comprador(r), 2026)
        self.assertEqual(list(linha), coletor.COLUNAS_CONTRATACOES)
        self.assertEqual(linha["comprador_cnpj"], "00394700000108")
        self.assertEqual(linha["comprador_codigo_unidade"], "926001")
        self.assertEqual(linha["comprador_esfera"], "Distrital")
        self.assertEqual(linha["comprador_poder"], "Executivo")
        self.assertEqual(linha["forma_submissao"], "electronicSubmission")
        self.assertEqual((linha["qtd_itens"], linha["qtd_adjudicacoes"]), (2, 1))
        self.assertEqual(linha["valor_adjudicado_total"], 800.12)

    def test_linhas_itens(self):
        itens = list(coletor.linhas_itens(release("ocds-1")))
        self.assertEqual([list(i) for i in itens], [coletor.COLUNAS_ITENS] * 2)
        self.assertEqual(itens[0]["codigo_catalogo"], "267502")
        self.assertEqual(itens[1]["codigo_catalogo"], "999")  # sem classification: usa o lote
        self.assertEqual(itens[0]["valor_unitario_estimado"], 0.5)
        self.assertEqual(itens[0]["categoria_nome"], "Medicamentos")

    def test_linhas_adjudicacoes(self):
        linhas = list(coletor.linhas_adjudicacoes(release("ocds-1")))
        self.assertEqual([l["fornecedor_id"] for l in linhas], ["11111111000111", "22222222000122"])
        self.assertEqual(linhas[0]["valor_total_item"], 3.33)
        self.assertEqual(list(linhas[0]), coletor.COLUNAS_ADJUDICACOES)

    def test_adjudicacao_sem_fornecedor(self):
        r = release("ocds-1")
        r["awards"][0]["suppliers"] = []
        linhas = list(coletor.linhas_adjudicacoes(r))
        self.assertEqual(len(linhas), 1)
        self.assertIsNone(linhas[0]["fornecedor_id"])


class ComPasta(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.pasta = Path(tmp.name)


class TestProcessaAno(ComPasta):
    def processa(self, filtro, limite=None):
        arquivo = self.pasta / "entrada.jsonl.gz"
        arquivo.write_bytes(CORPO)
        csvs = {nome: coletor.EscritorCSV(self.pasta / f"{nome}.csv", colunas)
                for nome, colunas in coletor.CSVS.items()}
        saida = StringIO()
        with self.assertLogs("coleta_contratacoes", "INFO") if limite else nullcontext():
            contagem = coletor.processa_ano(2026, arquivo, filtro, saida, csvs, limite)
        for escritor in csvs.values():
            escritor.__exit__()
        return contagem, saida.getvalue()

    def test_seleciona_df_e_registra_erro(self):
        contagem, jsonl = self.processa(coletor.Filtro("DF"))
        self.assertEqual(contagem, {"linhas_lidas": 7, "contratacoes_selecionadas": 3,
                                    "itens": 6, "adjudicacoes": 6, "erros": 1})
        ocids = [json.loads(l)["ocid"] for l in jsonl.splitlines()]
        self.assertEqual(ocids, ["ocds-1", "ocds-2", "ocds-5"])
        self.assertEqual([l["ocid"] for l in le_csv(self.pasta / "contratacoes.csv")], ocids)
        erros = le_csv(self.pasta / "erros.csv")
        self.assertEqual((erros[0]["linha"], erros[0]["etapa"]), ("5", "leitura"))
        self.assertIn("JSONDecodeError", erros[0]["erro"])

    def test_esfera(self):
        contagem, _ = self.processa(coletor.Filtro("DF", {"Federal"}))
        self.assertEqual(contagem["contratacoes_selecionadas"], 1)

    def test_limite(self):
        contagem, _ = self.processa(coletor.Filtro("DF"), limite=2)
        self.assertEqual((contagem["linhas_lidas"], contagem["contratacoes_selecionadas"]), (2, 2))


class TestMain(ComPasta):
    def setUp(self):
        super().setUp()
        self.servidor = ServidorLocal()
        self.addCleanup(self.servidor.fecha)
        for contexto in (patch.object(coletor, "URL_DOWNLOAD", self.servidor.url),
                         patch.object(coletor, "TIMEOUT", 5.0),
                         patch.object(coletor, "PASTA_SAIDA_PADRAO", self.pasta / "padrao"),
                         patch("sys.stdout", StringIO())):
            contexto.start()
            self.addCleanup(contexto.stop)
        self.saida = self.pasta / "saida"

    def executa(self, *argumentos):
        return coletor.main(["--anos", str(ANO), "--saida", str(self.saida), *argumentos])

    def test_sucesso(self):
        self.assertEqual(self.executa(), 0)
        self.assertEqual(sorted(p.name for p in self.saida.iterdir()), sorted(
            ["contratacoes.jsonl.gz", "coleta.log", *(f"{n}.csv" for n in coletor.CSVS)]))
        self.assertEqual(self.servidor.caminhos, [f"/download?name={ANO}.jsonl.gz"])
        self.assertEqual(self.servidor.cabecalhos_recebidos[0]["Accept-Encoding"], "identity")
        self.assertEqual(len(le_csv(self.saida / "contratacoes.csv")), 3)
        monitor = le_csv(self.saida / "monitoramento.csv")
        self.assertEqual((monitor[0]["status"], monitor[0]["arquivo_bytes"]), ("ok", str(len(CORPO))))
        with gzip.open(self.saida / "contratacoes.jsonl.gz", "rt", encoding="utf-8") as arquivo:
            self.assertEqual(len(arquivo.readlines()), 3)

    def test_saida_padrao(self):
        self.assertEqual(coletor.main(["--anos", str(ANO), "--esferas", "Distrital"]), 0)
        pastas = list((self.pasta / "padrao" / f"DF-Distrital-{ANO}").iterdir())
        self.assertEqual(len(pastas), 1)
        self.assertRegex(pastas[0].name, r"^\d{8}_\d{6}$")

    def test_saida_relativa_ao_diretorio_atual(self):
        anterior = os.getcwd()
        os.chdir(self.pasta)
        try:
            self.assertEqual(coletor.main(["--anos", str(ANO), "--saida", "rel/sub"]), 0)
        finally:
            os.chdir(anterior)
        self.assertTrue((self.pasta / "rel" / "sub" / "contratacoes.csv").exists())

    def test_saida_ocupada_recusada_sem_acessar_a_fonte(self):
        self.saida.mkdir()
        (self.saida / "antigo.csv").write_text("x")
        with patch("sys.stderr", StringIO()) as erro:
            self.assertEqual(self.executa(), 2)
        self.assertIn("já existe", erro.getvalue())
        self.assertEqual(self.servidor.caminhos, [])
        self.assertEqual([p.name for p in self.saida.iterdir()], ["antigo.csv"])

    def test_saida_vazia_aceita(self):
        self.saida.mkdir()
        self.assertEqual(self.executa(), 0)

    def test_404_registra_falha(self):
        self.servidor.status, self.servidor.corpo = 404, b""
        self.assertEqual(self.executa(), 1)
        self.assertEqual(le_csv(self.saida / "monitoramento.csv")[0]["status"], "falha")
        self.assertIn("HTTPError", le_csv(self.saida / "erros.csv")[0]["erro"])
        self.assertFalse(any(p.name.startswith(".download-") for p in self.saida.iterdir()))

    def test_download_truncado(self):
        self.servidor.content_length = len(CORPO) + 100
        self.assertEqual(self.executa(), 1)
        # Quem detecta primeiro é o urllib3 (IncompleteRead); a conferência do
        # Content-Length no coletor é a segunda barreira.
        self.assertRegex(le_csv(self.saida / "erros.csv")[0]["erro"], "IncompleteRead|incompleto")
        self.assertEqual(le_csv(self.saida / "monitoramento.csv")[0]["status"], "falha")
        self.assertFalse(any(p.name.startswith(".download-") for p in self.saida.iterdir()))

    def test_argumentos_invalidos(self):
        for argv in (["--anos", "2020"], ["--anos", str(ANO + 1)], ["--limite", "0"], ["--uf", " "],
                     ["--alias", "x"]):
            with self.subTest(argv=argv), patch("sys.stderr", StringIO()), self.assertRaises(SystemExit) as erro:
                coletor.main(argv)
            self.assertEqual(erro.exception.code, 2)
        self.assertEqual(self.servidor.caminhos, [])


def bash_compativel():
    """Retorna um bash que aceite caminhos do Windows (Git Bash), ou None."""
    if os.name != "nt":
        return shutil.which("bash")
    for pasta in (os.environ.get("ProgramFiles"), os.environ.get("ProgramW6432")):
        if pasta and (Path(pasta) / "Git" / "bin" / "bash.exe").exists():
            return str(Path(pasta) / "Git" / "bin" / "bash.exe")
    caminho = shutil.which("bash")
    return caminho if caminho and "\\git\\" in caminho.lower() else None


@unittest.skipUnless(bash_compativel(), "bash compatível indisponível")
class TestWrapper(unittest.TestCase):
    def test_wrapper_de_outro_diretorio_com_saida_relativa(self):
        servidor = ServidorLocal()
        self.addCleanup(servidor.fecha)
        ambiente = {**os.environ, "OCP_COMPRAS_DOWNLOAD_URL": servidor.url, "DOWNLOAD_TIMEOUT_SECONDS": "5"}
        with tempfile.TemporaryDirectory() as tmp:
            processo = subprocess.run(
                [bash_compativel(), WRAPPER.as_posix(), "--anos", str(ANO), "--saida", "outputs/x"],
                cwd=tmp, env=ambiente, capture_output=True, text=True, encoding="utf-8", timeout=120,
            )
            self.assertEqual(processo.returncode, 0, processo.stdout + processo.stderr)
            self.assertEqual(len(le_csv(Path(tmp) / "outputs" / "x" / "contratacoes.csv")), 3)
            self.assertIn("Coleta de contratações", processo.stdout)  # UTF-8 mesmo em pipe
        self.assertEqual(servidor.caminhos, [f"/download?name={ANO}.jsonl.gz"])

    def test_wrapper_argumento_invalido(self):
        with tempfile.TemporaryDirectory() as tmp:
            processo = subprocess.run([bash_compativel(), WRAPPER.as_posix(), "--anos", "2020"],
                                      cwd=tmp, capture_output=True, text=True, encoding="utf-8", timeout=60)
        self.assertEqual(processo.returncode, 2, processo.stderr)
        self.assertIn("--anos", processo.stderr)


if __name__ == "__main__":
    unittest.main()
