# coletor-contratacoes

Baixa os processos de contratação do Compras.gov.br em formato
[OCDS](https://standard.open-contracting.org/), mantém só os de compradores de
uma UF (padrão: DF) e os grava em CSVs prontos para análise, junto com as
releases OCDS originais selecionadas.

Criado a partir do [python-template-tb](https://github.com/Transparencia-Brasil/python-template-tb).
É usado pela task `coleta-contratacoes` do repositório `medicamentos-fdcc`.

## Fonte e abrangência

- [OCP Data Registry, publicação 157](https://data.open-contracting.org/en/publication/157):
  dados do Compras.gov.br (SEGES/MGI), sob licença CC BY 4.0.
- Um arquivo `.jsonl.gz` por ano, de 2021 em diante, atualizado mensalmente pela
  OCP. Cada linha é um processo (release compilada) com a licitação (`tender`) e
  as adjudicações (`awards`).
- Cada arquivo cobre o país todo: 154 MB em 2026 (202 mil processos, no arquivo
  baixado em 03/11/2026) e 379 MB em 2025. Por isso o coletor baixa o ano inteiro e filtra localmente.

## Filtro

Um processo é mantido quando a parte compradora (`buyer`) tem `address.region`
igual à UF pedida, sem diferenciar maiúsculas de minúsculas. Por padrão entram
todas as esferas (federal, distrital e outras). A esfera fica na coluna
`comprador_esfera`, para filtrar depois. Para restringir já na coleta, use
`--esferas`.

No arquivo de 2026 baixado em 03/11/2026, foram 12.594 processos no DF, 1.459
deles distritais.

Antes do `json.loads`, um pré-filtro textual descarta as linhas em que a UF nem
aparece entre aspas (`"DF"`). Elas falhariam no filtro de qualquer forma, e
assim só cerca de 6% das linhas precisam ser interpretadas. Com o arquivo já
baixado, processar 2026 leva cerca de 20 s.

## Instalação

Requer Python **3.10 ou superior**. Na raiz do repositório:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

No Linux/macOS, ative o ambiente com `source .venv/bin/activate`.

## Execução

```powershell
python src/ETL/coletores/coleta_contratacoes.py --anos 2025 2026
python src/ETL/coletores/coleta_contratacoes.py --anos 2026 --esferas Distrital
python src/ETL/coletores/coleta_contratacoes.py --anos 2026 --limite 5000   # teste rápido
```

| Opção | Padrão | Para quê |
| --- | --- | --- |
| `--anos` | ano atual | Anos a baixar, de 2021 ao ano atual. |
| `--uf` | `DF` | UF do comprador. |
| `--esferas` | todas | Esferas a manter: `Federal`, `Distrital`, `Estadual`, `Municipal`, `"Não se Aplica"`. |
| `--saida` | ver abaixo | Pasta de destino. |
| `--limite` | — | Lê só as N primeiras linhas de cada ano. |

Pelo Bash, de qualquer diretório, use os mesmos argumentos:

```bash
bash src/ETL/coletores/run-coleta-contratacoes.sh --anos 2026 --esferas Distrital
```

O wrapper usa o Python do `.venv` do repositório quando ele existe e, se não
existir, o `python3` ou `python` do sistema. Funciona no Git Bash e no WSL: no
WSL, com o `.venv` criado no Windows, o caminho do script é convertido com
`wslpath`. O wrapper não muda de diretório, então um `--saida` relativo é
resolvido a partir de quem chama.

## Saída

Sem `--saida`, cada execução grava numa pasta nova em `outputs/contratacoes/`, na
raiz do repositório:

```text
outputs/contratacoes/{UF}[-{esferas}]-{anos}/{AAAAMMDD}_{HHMMSS}/
outputs/contratacoes/DF-2025-2026/20261103_142300/
outputs/contratacoes/DF-Distrital-2026/20261103_142300/
```

Com `--saida`, a pasta informada é usada no lugar desse caminho. Em qualquer
caso, ela pode não existir ou estar vazia. Se já tiver arquivos, a coleta é
recusada com código 2, sem acessar a fonte, e nada é sobrescrito.

| Arquivo | Granularidade | Chave |
| --- | --- | --- |
| `contratacoes.jsonl.gz` | release OCDS original, só as selecionadas | `ocid` |
| `contratacoes.csv` | um processo de contratação | `ocid` |
| `itens.csv` | um item licitado (`tender.items` + dados do lote) | `ocid` + `item_id` |
| `adjudicacoes.csv` | um item adjudicado × fornecedor | `ocid` + `adjudicacao_id` + `item_id` + `fornecedor_id` |
| `monitoramento.csv` | um ano processado (linhas lidas, selecionadas, duração, status) | `ano` |
| `erros.csv` | linha ou ano com falha | — |
| `coleta.log` | log da execução (também vai para a saída padrão, em UTF-8) | — |

Em `itens.csv`, `codigo_catalogo` é o código CATMAT/CATSER.

O arquivo de cada ano é baixado em streaming para uma pasta temporária
`.download-*` dentro do destino. Ela é apagada ao final, mesmo em caso de falha,
então toda execução baixa o ano de novo. O download confere o total recebido com
o `Content-Length` informado pela fonte.

Um ano com falha (rede, HTTP, download incompleto) é registrado em `erros.csv` e
em `monitoramento.csv` com status `falha`, e a coleta segue para os anos
seguintes. Linhas que não podem ser lidas ou transformadas vão para `erros.csv`
com o número da linha e não interrompem o ano.

## Códigos de saída

| Código | Situação |
| --- | --- |
| **0** | Todos os anos processados. |
| **1** | Algum ano falhou no download ou na leitura. |
| **2** | Argumento inválido ou pasta de destino com arquivos. |

## Configuração

As variáveis podem ser definidas no ambiente ou num `.env` na raiz do
repositório, usando `.env.example` como referência. O ambiente tem precedência.

| Variável | Padrão | Descrição |
| --- | --- | --- |
| `OCP_COMPRAS_DOWNLOAD_URL` | `https://data.open-contracting.org/en/publication/157/download` | Endereço de download da publicação; o ano vai em `?name={ano}.jsonl.gz`. |
| `DOWNLOAD_TIMEOUT_SECONDS` | `60` | Timeout, por tentativa, para conectar e entre leituras da resposta. |

Falhas de conexão e respostas HTTP **429, 500, 502, 503 e 504** são repetidas até
oito vezes, com espera crescente de fator 2 e respeito ao cabeçalho
`Retry-After`.

## Testes

```powershell
python -m unittest discover -s tests -v
```

Os testes não acessam a OCP: usam um servidor HTTP local com um `.jsonl.gz`
pequeno. Cobrem o filtro (UF, esfera, comprador sem `buyer.id`), o pré-filtro, o
achatamento em contratações, itens e adjudicações, linhas inválidas, `--limite`,
a pasta de saída (padrão, relativa, vazia e ocupada), 404, download truncado,
remoção do temporário e o wrapper Bash chamado de outro diretório.
