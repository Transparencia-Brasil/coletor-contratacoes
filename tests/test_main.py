"""Testes do ponto de entrada da aplicação."""

import unittest
from contextlib import redirect_stdout
from io import StringIO

from src.main import main


class TestMain(unittest.TestCase):
    def test_main_exibe_mensagem_inicial(self) -> None:
        output = StringIO()

        with redirect_stdout(output):
            main()

        self.assertEqual(output.getvalue(), "Olá, Transparência Brasil!\n")


if __name__ == "__main__":
    unittest.main()
