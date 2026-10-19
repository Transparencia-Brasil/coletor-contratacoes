# python-template-tb

Template da Transparência Brasil para projetos de análise de dados em Python.

## Estrutura

```text
python-template-tb/
├── src/
│   ├── __init__.py
│   └── main.py
├── tasks/
├── tests/
│   ├── __init__.py
│   └── test_main.py
├── .gitignore
├── requirements.txt
└── README.md
```

## Preparação do ambiente

Com Python 3 instalado, execute na raiz do projeto:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

No Linux ou macOS, ative o ambiente com `source .venv/bin/activate`.
Caso o sistema disponibilize o interpretador como `python3`, use esse comando
para criar o ambiente virtual.

O template utiliza apenas a biblioteca padrão. Adicione dependências externas
ao `requirements.txt` conforme a necessidade do projeto.

## Execução

```powershell
python -m src.main
```

Implemente o fluxo principal em `src/main.py` e mantenha os demais módulos em `src/`.

## Testes

```powershell
python -m unittest discover -s tests -v
```

Adicione testes em arquivos `test_*.py` dentro de `tests/`.
