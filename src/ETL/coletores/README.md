# O que o repositório precisa

Seguindo o padrão do `cesta-de-precos-pncp`:

- **Estrutura:** `src/ETL/coletores/` com um script por fonte:
  - `coleta_contratacoes_ocds.py`
  - `coleta_contratos.py`
  - `coleta_empenhos_df.py`
- **Funções comuns:** um `utils.py` que replique o `coleta_endpoint()` do R, ou seja, intervalo entre requisições, novas tentativas com espera crescente e tratamento de HTTP 204.
- **Saídas:** cada coleta gera `dados.csv`, `erros.csv` e `monitoramento.csv` em `coleta/<fonte>/<ALIAS>/`.
- **Orquestrador:** um `run-coletores.sh` com os parâmetros `ALIAS`, data de início e data de fim, e log em `src/ETL/log/`.
- **Dependências:** um `requirements.txt` com `requests` ou `httpx`, `tenacity` (novas tentativas) e `pandas`. O `flattentool`, que o repositório antigo já usa para OCDS, também serve.
- **Configuração:** `.gitignore` para a pasta `coleta/` e um `.env` para variáveis como o intervalo entre requisições.
- **Opcional:** reaproveitar o `tasks/mapeamento-ocds` do repositório antigo, que já transforma OCDS em CSV com Python.

## O que o repositório tem hoje

- **Python já é usado.** O `.venv` e o `.env` ficam na raiz, e o `.env.example` já tem `COMPRAS_API_BASE_URL` e `API_TIMEOUT_SECONDS`. Então dá para fazer os coletores em Python sem atrito.
- **`tasks/analise-inicial-csv-e-api/`** tem o `consulta_api.py`, que consulta `/modulo-ocds/1_releases` (OCDS por CNPJ do comprador e datas) com `requests` e `python-dotenv`. Tem também um CSV do PNCP com 1.515 contratações de compradores no DF, 320 delas distritais. O foco do projeto é o DF.
- **`tasks/mapeamento-ocds/`** é uma cópia da task do `cesta-de-precos-pncp`. Ela depende das pastas `coleta/` daquele repositório, então aqui não roda sozinha.

## O que é preciso para criar os coletores

1. **Funções HTTP compartilhadas** (intervalo entre requisições, novas tentativas, log). O modelo não tem lugar para código comum. Sugiro um `setup/py/utils_http.py`, que também pode absorver a lógica do `consulta_api.py`.

2. **Um `requirements.txt` na raiz.** Hoje ele só existe dentro de uma task. Bastam `requests`, `python-dotenv`, `pandas` e `tenacity`.

3. **Novas variáveis no `.env.example`:**
   - a URL de download da OCP;
   - o identificador do conjunto de despesas no portal do DF;
   - o intervalo entre requisições.

4. **Entradas no `.gitignore`** para os `outputs/` dos coletores, que vão ficar pesados.

# Coleta de contratações (OCDS · Compras.gov.br)

Baixa os processos de contratação do Compras.gov.br em formato [OCDS](https://standard.open-contracting.org/) e mantém só os de compradores sediados no **DF**.

## Fonte

[OCP Data Registry, publicação 157](https://data.open-contracting.org/en/publication/157). Os dados são do Compras.gov.br (SEGES/MGI), com licença CC BY 4.0. A OCP disponibiliza um arquivo `.jsonl.gz` por ano, de 2021 em diante, e o atualiza mensalmente. Cada linha é um processo (release compilada) com a licitação (`tender`) e as adjudicações (`awards`).

O arquivo do ano cobre o país todo. Por isso o coletor baixa o ano inteiro para `tmp/`, lê linha a linha e filtra.

## Filtro

O coletor mantém o processo quando a parte compradora (`buyer`) tem `address.region == "DF"`. Isso inclui todas as esferas: federal, distrital e outras. A esfera vem na coluna `comprador_esfera` de `contratacoes.csv`, para filtrar depois. Para restringir já na coleta, use `--esferas Distrital`.

Em 2026, até a atualização de set/2026, o arquivo trouxe 11.732 processos no DF: 9.693 federais, 1.386 distritais, 648 de outras esferas ("Não se Aplica", como a EBSERH) e 5 estaduais.

## Como rodar

Execute os comandos a partir da raiz do repositório, com o ambiente Python (`.venv`) ativado.

Os exemplos executam o coletor diretamente: o wrapper `src/ETL/coletores/run-coleta-contratacoes.sh` ainda referencia o caminho antigo em `tasks/`.

```bash
python -m pip install -r requirements.txt

# um ou mais anos
python src/ETL/coletores/coleta_contratacoes.py --anos 2025 2026

# filtrar por esfera e definir a pasta de saída
python src/ETL/coletores/coleta_contratacoes.py --anos 2026 --esferas Distrital --alias DF-distrital-2026

# teste rápido: lê só as primeiras 5.000 linhas de cada ano
python src/ETL/coletores/coleta_contratacoes.py --anos 2026 --limite 5000 --alias teste
```

| Opção | Padrão | Para quê |
| --- | --- | --- |
| `--anos` | ano atual | Anos a baixar (2021 em diante). |
| `--uf` | `DF` | UF do comprador. |
| `--esferas` | todas | Esferas a manter: `Federal`, `Distrital`, `Estadual`, `Municipal`, `"Não se Aplica"`. |
| `--alias` | `DF-<anos>` | Subpasta de saída em `outputs/`. |
| `--manter-download` | não | Mantém o `.jsonl.gz` em `tmp/` (por padrão é apagado se o ano der certo). |
| `--forcar-download` | não | Baixa de novo mesmo que o arquivo já esteja em `tmp/`. |
| `--limite` | — | Lê só as N primeiras linhas de cada ano. |

Os downloads são grandes: 146 MB para 2026 e 379 MB para 2025. Com o arquivo já baixado, processar 2026 leva cerca de 30 s.

## Saídas

Com a configuração atual do coletor, ficam em `src/ETL/outputs/<alias>/`, e os downloads temporários em `src/ETL/tmp/`.

| Arquivo | Granularidade | Chave |
| --- | --- | --- |
| `contratacoes.jsonl.gz` | release OCDS original, só as selecionadas | `ocid` |
| `contratacoes.csv` | um processo de contratação | `ocid` |
| `itens.csv` | um item licitado (`tender.items` + dados do lote) | `ocid` + `item_id` |
| `adjudicacoes.csv` | um item adjudicado × fornecedor | `ocid` + `adjudicacao_id` + `item_id` + `fornecedor_id` |
| `monitoramento.csv` | um ano processado (linhas lidas, selecionadas, duração, status) | `ano` |
| `erros.csv` | linha ou ano com falha | — |
| `coleta.log` | log da execução | — |

Em `itens.csv`, `codigo_catalogo` é o código CATMAT/CATSER. Ele serve para identificar medicamentos depois, por exemplo cruzando com `tasks/mapeamento-ocds/input/dicionario-medicamentos-ocds.csv`.