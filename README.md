# coletor-contratacoes

Baixa o arquivo anual de contratações do Compras.gov.br em formato
[OCDS](https://standard.open-contracting.org/) (JSON), verifica se ele chegou
íntegro e o salva exatamente como veio. O coletor não filtra, não converte e não reescreve
dados.

Criado a partir do [python-template-tb](https://github.com/Transparencia-Brasil/python-template-tb).
É usado pela task `coleta-contratacoes` do repositório `medicamentos-fdcc`, onde
fica o tratamento dos dados baixados.

## Fonte

[OCP Data Registry, publicação 157](https://data.open-contracting.org/en/publication/157):
compras públicas do Governo Federal no sistema Compras.gov.br, publicadas pela
SEGES/MGI, sob licença CC BY 4.0.

- É a fonte oficial, coletada pela OCP todo mês. Cobre as etapas de licitação e
  de adjudicação de todos os órgãos que usam o Compras.gov.br: federais e os
  estados e municípios que aderiram ao sistema.
- Traz todos os itens comprados, não só medicamentos. Separar os medicamentos é
  trabalho do tratamento, no `medicamentos-fdcc`.
- Há um arquivo `{ano}.jsonl.gz` por ano, de 2021 em diante, com um processo de
  contratação (release compilada OCDS) por linha. O de 2026 tinha 154 MB
  compactado, 2,35 GB descompactado e 202 mil processos em 04/11/2026.

A OCP redireciona o download para o CDN dela, e a URL final traz o número da
versão da publicação, por exemplo `.../brazil_compras/4322/2026.jsonl.gz`. A
fonte não publica checksum.

Outras publicações da OCP com o mesmo padrão de URL podem ser baixadas trocando
`OCP_DOWNLOAD_URL` (ver [Configuração](#configuração)).

## Instalação

Requer Python **3.10 ou superior**. Na raiz do repositório:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

No Linux/macOS, ative o ambiente com `source .venv/bin/activate`.

## Execução

Cada execução baixa um ano:

```powershell
python src/ETL/coletores/coleta_contratacoes.py --ano 2026
python src/ETL/coletores/coleta_contratacoes.py --ano 2026 --saida caminho/2026.jsonl.gz
```

| Opção | Obrigatória | Para quê |
| --- | --- | --- |
| `--ano` | sim | Ano com quatro dígitos. Se o ano tem arquivo é a fonte que diz: um ano sem arquivo recebe 404. |
| `--saida` | não | Caminho completo do arquivo a gravar. |

Pelo Bash, de qualquer diretório, use os mesmos argumentos:

```bash
bash src/ETL/coletores/run-coleta-contratacoes.sh --ano 2026
```

O wrapper usa o Python do `.venv` do repositório quando ele existe e, se não
existir, o `python3` ou `python` do sistema. Funciona no Git Bash e no WSL: no
WSL, com o `.venv` criado no Windows, o caminho do script é convertido com
`wslpath`. O wrapper não muda de diretório, então um `--saida` relativo é
resolvido a partir de quem chama.

## Saída

Sem `--saida`, cada execução grava um arquivo novo, na raiz do repositório:

```text
outputs/contratacoes/{ano}/{AAAAMMDD}_{HHMMSS}_contratacoes_{ano}.jsonl.gz
outputs/contratacoes/2026/20261104_153000_contratacoes_2026.jsonl.gz
```

Data e hora são as do início da execução, no horário local, e vêm primeiro para
que a ordem alfabética seja a cronológica.

Com `--saida`, o caminho informado é usado no lugar desse. As pastas que
faltarem são criadas, só depois de a fonte responder 200: um ano não publicado
não deixa pasta vazia. Em qualquer caso, se o destino já existir, seja arquivo ou
pasta, a coleta é recusada com código 2, sem acessar a fonte, e nada é
sobrescrito.

O log vai só para a saída padrão, em UTF-8. Ao final, ele registra a URL pedida
e a URL final, os bytes gravados e o `Content-Length`, o `Last-Modified`, o
SHA-256 do arquivo, o tamanho descompactado e o tempo do download e da
verificação.

## Integridade

O arquivo é baixado em streaming para `{destino}.part`, na mesma pasta do
destino. Só vira o arquivo final depois de passar por duas verificações:

1. O total recebido é igual ao `Content-Length`, quando a fonte o informa.
2. O gzip é lido inteiro, sem gravar o conteúdo. A leitura confere o CRC32 e o
   tamanho original gravados no próprio arquivo, então falha com um arquivo
   vazio, cortado, corrompido, com dados depois do fim do gzip ou que não seja
   gzip (uma página de erro, por exemplo). As linhas não são interpretadas como
   JSON.

Em qualquer falha ou interrupção, o `.part` é apagado e o destino não é criado.
Com o arquivo de 2026 (154 MB), a verificação leva cerca de 5 s.

## Códigos de saída

| Código | Situação |
| --- | --- |
| **0** | Arquivo salvo e íntegro. |
| **1** | Falha de rede, HTTP diferente de 200 e de 404, download incompleto, gzip inválido ou erro de escrita. |
| **2** | Argumento ou configuração inválidos, ano não publicado (404) ou destino já existente. |

## Configuração

As variáveis podem ser definidas no ambiente ou num `.env` na raiz do
repositório, usando `.env.example` como referência. O ambiente tem precedência,
e uma variável definida mas vazia usa o padrão. Um valor inválido encerra com
código 2 antes de acessar a fonte.

| Variável | Padrão | Descrição |
| --- | --- | --- |
| `OCP_DOWNLOAD_URL` | `https://data.open-contracting.org/en/publication/157/download` | Endereço de download da publicação; o ano vai em `?name={ano}.jsonl.gz`. HTTP(S), sem query ou fragmento. |
| `DOWNLOAD_TIMEOUT_SECONDS` | `60` | Timeout, por tentativa, para conectar e entre leituras da resposta. Número positivo e finito. |

`OCP_DOWNLOAD_URL` substitui a antiga `OCP_COMPRAS_DOWNLOAD_URL`, que deixou de
ser lida.

Falhas de conexão e respostas HTTP **429, 500, 502, 503 e 504** são repetidas até
oito vezes, com espera crescente de fator 2 e respeito ao cabeçalho
`Retry-After`. Uma falha no meio da leitura do corpo não é repetida.

## Testes

```powershell
python -m unittest discover -s tests -t . -v
```

Os testes não acessam a OCP: usam um servidor HTTP local. Cobrem:

- a configuração (padrão, ambiente, valores inválidos e variáveis vazias) e os
  argumentos;
- o download com sucesso, com redirecionamento, sem `Content-Length` e com
  `Content-Encoding`;
- 404 (sem criar a pasta do destino) e 500;
- download truncado, gzip com CRC errado, resposta que não é gzip, dados depois
  do gzip e corpo vazio;
- a interrupção durante a verificação e, em todas as falhas, a remoção do `.part`;
- o destino padrão, o `--saida` relativo e o destino existente;
- o wrapper Bash chamado de outro diretório.
