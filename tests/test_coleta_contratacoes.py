"""Testes do coletor de contratações, sem acesso à OCP."""

import gzip
import hashlib
import os
import shutil
import subprocess
import tempfile
import threading
import unittest
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import StringIO
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import requests

from src.ETL.coletores import coleta_contratacoes as coletor

URL_157 = "https://data.open-contracting.org/en/publication/157/download"
LAST_MODIFIED = "Sun, 01 Nov 2026 00:10:23 GMT"
CORPO = gzip.compress(b'{"ocid": "ocds-1"}\n{"ocid": "ocds-2"}\n')


class ServidorLocal:
    """Servidor HTTP em thread que serve sempre o mesmo corpo, como a fonte.

    Com `redireciona`, /download responde 302 para /cdn/<name>, como a OCP faz
    para o CDN. Com `sem_content_length`, o corpo termina com o fechamento da
    conexão. `content_length` força um tamanho declarado diferente do real.
    """

    def __init__(self):
        self.status = 200
        self.corpo = CORPO
        self.content_length = None
        self.sem_content_length = False
        self.redireciona = False
        self.content_encoding = None
        self.caminhos = []
        self.cabecalhos_recebidos = []
        servidor = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                servidor.caminhos.append(self.path)
                servidor.cabecalhos_recebidos.append(dict(self.headers))
                if servidor.redireciona and self.path.startswith("/download"):
                    nome = parse_qs(urlsplit(self.path).query)["name"][0]
                    self.send_response(302)
                    self.send_header("Location", f"/cdn/{nome}")
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                self.send_response(servidor.status)
                if not servidor.sem_content_length:
                    self.send_header("Content-Length", str(servidor.content_length or len(servidor.corpo)))
                self.send_header("Last-Modified", LAST_MODIFIED)
                if servidor.content_encoding:
                    self.send_header("Content-Encoding", servidor.content_encoding)
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


class ComServidor(unittest.TestCase):
    """Pasta temporária e servidor local para cada teste."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.pasta = Path(tmp.name)
        self.servidor = ServidorLocal()
        self.addCleanup(self.servidor.fecha)

    def sem_restos(self, destino):
        """O destino não foi criado e nenhum .part sobrou."""
        self.assertFalse(destino.exists())
        self.assertEqual(list(self.pasta.rglob("*.part")), [])


class TestConfiguracaoEArgumentos(unittest.TestCase):
    def ambiente(self, url="", timeout=""):
        return patch.dict(os.environ, {"OCP_DOWNLOAD_URL": url, "DOWNLOAD_TIMEOUT_SECONDS": timeout})

    def test_configuracao_padrao(self):
        with self.ambiente():
            self.assertEqual(coletor.configuracao(), coletor.Configuracao(URL_157, 60.0))

    def test_configuracao_do_ambiente(self):
        with self.ambiente("http://x/download", "5"):
            self.assertEqual(coletor.configuracao(), coletor.Configuracao("http://x/download", 5.0))

    def test_configuracao_invalida(self):
        casos = [{"url": u} for u in ("ftp://x/d", "x/d", "http://x/d?name=1")]
        casos += [{"timeout": t} for t in ("0", "-1", "abc", "inf")]
        for caso in casos:
            with self.subTest(**caso), self.ambiente(**caso), self.assertRaises(ValueError):
                coletor.configuracao()

    def test_url_do_ano(self):
        self.assertEqual(coletor.url_do_ano(coletor.Configuracao("http://x/download", 1.0), "2026"),
                         "http://x/download?name=2026.jsonl.gz")

    def test_destino_padrao(self):
        self.assertEqual(coletor.destino_padrao("2026", datetime(2026, 11, 4, 15, 30, 0)),
                         coletor.PASTA_SAIDA / "2026" / "20261104_153000_contratacoes_2026.jsonl.gz")

    def test_argumentos_validos(self):
        self.assertEqual(coletor.le_argumentos(["--ano", "2026"]), ("2026", None))
        self.assertEqual(coletor.le_argumentos(["--ano", "2026", "--saida", "a/b.jsonl.gz"]),
                         ("2026", Path("a/b.jsonl.gz")))

    def test_argumentos_invalidos(self):
        for argv in ([], ["--ano", "26"], ["--ano", "abcd"], ["--ano", "２０２６"],
                     ["--ano", "2026", "--uf", "DF"], ["--anos", "2026"], ["--ano", "2026", "--limite", "5"]):
            with self.subTest(argv=argv), patch("sys.stderr", StringIO()), self.assertRaises(SystemExit) as erro:
                coletor.le_argumentos(argv)
            self.assertEqual(erro.exception.code, 2)


class TestBaixaEVerifica(ComServidor):
    def baixa(self, sessao=None):
        self.destino = self.pasta / "sub" / "a.jsonl.gz"
        return coletor.baixa_e_verifica(sessao or coletor.sessao_http(), self.servidor.url + "?name=2026.jsonl.gz",
                                        self.destino, 5.0)

    def falha(self, excecao, sessao=None):
        with self.assertRaises(excecao):
            self.baixa(sessao)
        self.sem_restos(self.destino)

    def test_sucesso(self):
        download = self.baixa()
        self.assertEqual(self.destino.read_bytes(), CORPO)
        self.assertEqual((download.bytes_gravados, download.content_length), (len(CORPO), len(CORPO)))
        self.assertEqual(download.sha256, hashlib.sha256(CORPO).hexdigest())
        self.assertEqual(download.bytes_descompactados, len(gzip.decompress(CORPO)))
        self.assertEqual(download.last_modified, LAST_MODIFIED)
        self.assertEqual(self.servidor.cabecalhos_recebidos[0]["Accept-Encoding"], "identity")
        self.assertEqual(list(self.pasta.rglob("*.part")), [])

    def test_redirecionamento(self):
        self.servidor.redireciona = True
        download = self.baixa()
        self.assertEqual(self.destino.read_bytes(), CORPO)
        self.assertTrue(download.url_final.endswith("/cdn/2026.jsonl.gz"), download.url_final)

    def test_sem_content_length(self):
        self.servidor.sem_content_length = True
        download = self.baixa()
        self.assertEqual(self.destino.read_bytes(), CORPO)
        self.assertIsNone(download.content_length)

    def test_content_encoding_gzip_grava_os_bytes_recebidos(self):
        # Um CDN que marque o .gz com Content-Encoding: gzip não pode fazer o
        # coletor gravar o conteúdo descompactado.
        self.servidor.content_encoding = "gzip"
        download = self.baixa()
        self.assertEqual(self.destino.read_bytes(), CORPO)
        self.assertEqual(download.bytes_gravados, len(CORPO))

    def test_404(self):
        self.servidor.status = 404
        self.falha(coletor.AnoNaoPublicado)

    def test_404_nao_cria_a_pasta_do_destino(self):
        self.servidor.status = 404
        self.falha(coletor.AnoNaoPublicado)
        self.assertFalse(self.destino.parent.exists())

    def test_500(self):
        self.servidor.status = 500
        self.falha(requests.HTTPError, requests.Session())

    def test_truncado(self):
        self.servidor.content_length = len(CORPO) + 100
        self.falha((coletor.DownloadIncompleto, requests.RequestException))

    def test_crc_errado(self):
        corpo = bytearray(CORPO)
        corpo[-8] ^= 0xFF  # primeiro byte do CRC32 no fim do gzip
        self.servidor.corpo = bytes(corpo)
        self.falha(coletor.GzipInvalido)

    def test_nao_gzip(self):
        self.servidor.corpo = b"<html>erro</html>"
        self.falha(coletor.GzipInvalido)

    def test_lixo_depois_do_gzip(self):
        self.servidor.corpo = CORPO + b"lixo"
        self.falha(coletor.GzipInvalido)

    def test_corpo_vazio(self):
        self.servidor.corpo = b""
        self.falha(coletor.GzipInvalido)

    def test_interrupcao_na_verificacao(self):
        with patch.object(coletor, "verifica_gzip", side_effect=KeyboardInterrupt):
            self.falha(KeyboardInterrupt)


class TestMain(ComServidor):
    def setUp(self):
        super().setUp()
        for contexto in (patch.dict(os.environ, {"OCP_DOWNLOAD_URL": self.servidor.url,
                                                 "DOWNLOAD_TIMEOUT_SECONDS": "5"}),
                         patch.object(coletor, "PASTA_SAIDA", self.pasta / "padrao"),
                         patch("sys.stderr", StringIO())):  # mensagens de erro do logging
            contexto.start()
            self.addCleanup(contexto.stop)
        self.destino = self.pasta / "saida" / "a.jsonl.gz"

    def executa(self, *argumentos):
        return coletor.main(["--ano", "2026", "--saida", str(self.destino), *argumentos])

    def test_sucesso(self):
        with self.assertLogs("coleta_contratacoes", "INFO") as logs:
            self.assertEqual(self.executa(), 0)
        self.assertEqual(self.destino.read_bytes(), CORPO)
        texto = "\n".join(logs.output)
        self.assertIn(hashlib.sha256(CORPO).hexdigest(), texto)
        self.assertIn(self.servidor.url, texto)
        self.assertEqual(self.servidor.caminhos, ["/download?name=2026.jsonl.gz"])

    def test_destino_padrao(self):
        self.assertEqual(coletor.main(["--ano", "2026"]), 0)
        arquivos = [p.name for p in (self.pasta / "padrao" / "2026").iterdir()]
        self.assertEqual(len(arquivos), 1)
        self.assertRegex(arquivos[0], r"^\d{8}_\d{6}_contratacoes_2026\.jsonl\.gz$")

    def test_saida_relativa(self):
        anterior = os.getcwd()
        os.chdir(self.pasta)
        try:
            self.assertEqual(coletor.main(["--ano", "2026", "--saida", "rel/sub/a.jsonl.gz"]), 0)
        finally:
            os.chdir(anterior)
        self.assertEqual((self.pasta / "rel" / "sub" / "a.jsonl.gz").read_bytes(), CORPO)

    def test_destino_existente_recusado_sem_acessar_a_fonte(self):
        self.destino.parent.mkdir(parents=True)
        for tipo in ("arquivo", "pasta"):
            with self.subTest(tipo=tipo):
                if tipo == "arquivo":
                    self.destino.write_bytes(b"antigo")
                else:
                    self.destino.unlink()
                    self.destino.mkdir()
                self.assertEqual(self.executa(), 2)
                self.assertEqual(self.servidor.caminhos, [])
        self.assertTrue(self.destino.is_dir())

    def test_arquivo_existente_fica_intacto(self):
        self.destino.parent.mkdir(parents=True)
        self.destino.write_bytes(b"antigo")
        self.assertEqual(self.executa(), 2)
        self.assertEqual(self.destino.read_bytes(), b"antigo")

    def test_404_codigo_2(self):
        self.servidor.status = 404
        self.assertEqual(self.executa(), 2)
        self.sem_restos(self.destino)

    def test_500_codigo_1(self):
        self.servidor.status = 500
        with patch.object(coletor, "sessao_http", requests.Session):
            self.assertEqual(self.executa(), 1)
        self.sem_restos(self.destino)

    def test_gzip_invalido_codigo_1(self):
        self.servidor.corpo = b"<html>erro</html>"
        self.assertEqual(self.executa(), 1)
        self.sem_restos(self.destino)

    def test_configuracao_invalida_codigo_2(self):
        with patch.dict(os.environ, {"OCP_DOWNLOAD_URL": "ftp://x"}):
            self.assertEqual(self.executa(), 2)
        self.assertEqual(self.servidor.caminhos, [])

    def test_pasta_da_saida_e_um_arquivo(self):
        (self.pasta / "arquivo").write_text("x")
        self.assertEqual(coletor.main(["--ano", "2026", "--saida", str(self.pasta / "arquivo" / "a.jsonl.gz")]), 1)
        self.assertEqual(list(self.pasta.rglob("*.part")), [])


def bash_compativel():
    """Retorna um bash que aceite caminhos do Windows (Git Bash), ou None."""
    if os.name != "nt":
        return shutil.which("bash")
    for pasta in (os.environ.get("ProgramFiles"), os.environ.get("ProgramW6432")):
        if pasta and (Path(pasta) / "Git" / "bin" / "bash.exe").exists():
            return str(Path(pasta) / "Git" / "bin" / "bash.exe")
    caminho = shutil.which("bash")
    return caminho if caminho and "\\git\\" in caminho.lower() else None


RAIZ = Path(__file__).resolve().parents[1]
WRAPPER = RAIZ / "src" / "ETL" / "coletores" / "run-coleta-contratacoes.sh"


@unittest.skipUnless(bash_compativel(), "bash compatível indisponível")
class TestWrapper(unittest.TestCase):
    def test_wrapper_de_outro_diretorio_com_saida_relativa(self):
        servidor = ServidorLocal()
        self.addCleanup(servidor.fecha)
        ambiente = {**os.environ, "OCP_DOWNLOAD_URL": servidor.url, "DOWNLOAD_TIMEOUT_SECONDS": "5"}
        with tempfile.TemporaryDirectory() as tmp:
            processo = subprocess.run(
                [bash_compativel(), WRAPPER.as_posix(), "--ano", "2026", "--saida", "outputs/x/a.jsonl.gz"],
                cwd=tmp, env=ambiente, capture_output=True, text=True, encoding="utf-8", timeout=120,
            )
            self.assertEqual(processo.returncode, 0, processo.stdout + processo.stderr)
            self.assertEqual((Path(tmp) / "outputs" / "x" / "a.jsonl.gz").read_bytes(), CORPO)
            self.assertIn("Arquivo salvo", processo.stdout)
            self.assertIn("verificação", processo.stdout)  # UTF-8 mesmo em pipe
        self.assertEqual(servidor.caminhos, ["/download?name=2026.jsonl.gz"])

    def test_wrapper_argumento_invalido(self):
        with tempfile.TemporaryDirectory() as tmp:
            processo = subprocess.run([bash_compativel(), WRAPPER.as_posix(), "--ano", "26"],
                                      cwd=tmp, capture_output=True, text=True, encoding="utf-8", timeout=60)
        self.assertEqual(processo.returncode, 2, processo.stderr)
        self.assertIn("--ano", processo.stderr)


if __name__ == "__main__":
    unittest.main()
