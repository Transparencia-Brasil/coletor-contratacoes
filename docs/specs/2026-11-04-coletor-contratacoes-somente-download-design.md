# Coletor de contratações: somente download e verificação de integridade

Data: 2026-11-04. Status: implementado. Revisão 1 (2026-11-04): por alguns
commits o arquivo baixado foi o `{ano}.csv.tar.gz` da publicação 153. Revisão 2
(2026-11-04), decisão da equipe: a fonte passa a ser o `{ano}.jsonl.gz` da
publicação 157 (Compras.gov.br). A descompactação, a seleção dos medicamentos e
dos compradores do DF ficam no `medicamentos-fdcc`; o critério de medicamento
ainda será definido.

## Objetivo

Baixar, para um ano informado, o arquivo `{ano}.jsonl.gz` da publicação 157 do
OCP Data Registry (Compras.gov.br), verificar se ele chegou íntegro e salvá-lo
exatamente como veio. O coletor não filtra, não converte e não reescreve dados.

Antes desta especificação, o coletor filtrava por UF e esfera e gerava CSVs
(contratações, itens, adjudicações, monitoramento e erros). Esse tratamento sai
do coletor: descompactar o arquivo, separar os medicamentos e os compradores do
DF fica na task `coleta-contratacoes` do `medicamentos-fdcc`.

## Fonte

[Publicação 157 do OCP Data Registry](https://data.open-contracting.org/en/publication/157),
compras públicas do Governo Federal no sistema Compras.gov.br, publicadas pela
SEGES/MGI, sob licença CC BY 4.0:

- É a fonte oficial, coletada pela OCP todo mês. Cobre as etapas de licitação e
  de adjudicação de todos os órgãos que usam o Compras.gov.br.
- Traz todos os itens comprados, não só medicamentos.
- Vem em OCDS, uma release compilada por linha.

## Contexto verificado em 2026-11-04

- `GET https://data.open-contracting.org/en/publication/157/download?name=2026.jsonl.gz`
  responde `302` para
  `https://fastly.data.open-contracting.org/downloads/brazil_compras/4322/2026.jsonl.gz`.
  O número `4322` identifica a versão da publicação e muda a cada atualização.
- O CDN envia `Content-Length` (154.461.543 bytes em 2026),
  `Content-Type: application/gzip`, `Last-Modified` (27/10/2026) e `ETag`.
  Descompactado, o arquivo de 2026 tem 2.348.204.721 bytes e 202.324 processos.
  O de 2025 tem 379 MB compactado.
- Um ano não publicado (`2019`) responde `404`.
- A fonte não publica checksum. O `ETag` é formado pelo tamanho e pela data, não
  por um hash do conteúdo. A integridade é verificada pelo `Content-Length` e
  pelo CRC32 e pelo tamanho original gravados no próprio gzip.
- A classe do CATMAT (por exemplo 6505, medicamentos) não vem no arquivo: cada
  item traz só o código do item no catálogo (`classification.id`, esquema
  `CNBS`) e o lote traz a NCM. A categoria de catálogo dos lotes
  (`catalogItemCategoryName`) vem igual em todos os itens e não serve para
  separar medicamentos. Informação para o tratamento, não para o coletor.

## Interface

```text
python src/ETL/coletores/coleta_contratacoes.py --ano 2026
python src/ETL/coletores/coleta_contratacoes.py --ano 2026 --saida caminho/2026.jsonl.gz
bash   src/ETL/coletores/run-coleta-contratacoes.sh --ano 2026
```

- `--ano`: obrigatório, exatamente quatro dígitos ASCII (`[0-9]{4}`). Não há
  intervalo fixo no código: quem decide se o ano existe é a fonte, pelo 404.
- `--saida`: opcional, caminho completo do arquivo a gravar. Um caminho relativo é
  resolvido a partir do diretório de quem chama, já que o wrapper não muda de
  diretório. As pastas que faltarem são criadas.
- Saem `--anos`, `--uf`, `--esferas` e `--limite`. Para vários anos, quem chama
  executa o coletor uma vez por ano.
- O wrapper Bash não muda: repassa os argumentos e prefere o Python do `.venv`.

## Destino

Sem `--saida`:

```text
outputs/contratacoes/{ano}/{AAAAMMDD}_{HHMMSS}_contratacoes_{ano}.jsonl.gz
outputs/contratacoes/2026/20261104_153000_contratacoes_2026.jsonl.gz
```

Na raiz do repositório, independentemente do diretório de execução. Data e hora
são as do início da execução, no horário local. Cada execução gera um arquivo
novo e nada é sobrescrito.

Um destino que já existe, seja arquivo ou pasta, informado em `--saida` ou
gerado no mesmo segundo, é recusado com código 2 antes de acessar a fonte.

## Configuração

Lida do ambiente ou do `.env` na raiz. O ambiente tem precedência.

| Variável | Padrão | Uso |
| --- | --- | --- |
| `OCP_DOWNLOAD_URL` | `https://data.open-contracting.org/en/publication/157/download` | Endereço de download da publicação; o ano vai em `?name={ano}.jsonl.gz`. HTTP(S), com host, sem query nem fragmento. |
| `DOWNLOAD_TIMEOUT_SECONDS` | `60` | Segundos, por tentativa, para conectar e entre leituras da resposta. Número positivo e finito. |

Uma variável definida mas vazia usa o padrão. Um valor inválido encerra com
código 2 antes de acessar a fonte.

`OCP_DOWNLOAD_URL` substitui `OCP_COMPRAS_DOWNLOAD_URL`. A variável antiga deixa
de ser lida.

## Fluxo

1. Valida os argumentos e a configuração, monta a URL e o destino.
2. Se o destino já existe, encerra com código 2.
3. Faz o `GET` em streaming, com a sessão de hoje:
   - até 8 retentativas para falhas de conexão e HTTP 429, 500, 502, 503 e 504;
   - backoff com fator 2, respeitando `Retry-After`;
   - `Accept-Encoding: identity`.

   Uma falha no meio da leitura do corpo não é repetida.
4. HTTP 404 encerra com código 2, com a mensagem de ano não publicado. Qualquer
   outro status diferente de 200 encerra com código 1.
5. Cria a pasta do destino, se faltar, só agora: um 404 não deixa pasta vazia.
   Grava o corpo em blocos de 1 MiB em `{destino}.part`, na mesma pasta do
   destino, com os bytes crus da conexão (um `Content-Encoding` não é
   decodificado) e calculando o SHA-256 ao mesmo tempo.
6. Verifica a integridade:
   1. Se a fonte enviou `Content-Length`, o total gravado tem de ser igual a ele.
   2. O `.part` é lido inteiro com `gzip.open`, em blocos, e o resultado é
      descartado. O módulo confere o CRC32 e o tamanho original de cada membro
      do gzip. Arquivo cortado, corrompido, com lixo depois do gzip ou que não
      seja gzip falha aqui. Nada do conteúdo é interpretado: as linhas não são
      lidas como JSON.
7. Só depois das duas verificações, o `.part` substitui o destino de forma
   atômica (`Path.replace`).
8. Em qualquer falha ou interrupção, o `.part` é removido e o destino não é
   criado.

## Log

Vai só para a saída padrão, em UTF-8. Não há `coleta.log`: quem chama decide onde
guardar, e a task do `medicamentos-fdcc` já faz isso com `tee`. Ao final de um
download bem-sucedido, o log registra:

- a URL pedida e a URL final, depois do redirecionamento, que traz a versão da
  publicação;
- os bytes gravados e o `Content-Length`, ou a informação de que ele não foi
  enviado;
- o `Last-Modified`, ou a informação de que ele não foi enviado;
- o SHA-256 do arquivo;
- o tamanho descompactado;
- o tempo do download e o tempo da verificação.

## Códigos de saída

| Código | Situação |
| --- | --- |
| 0 | Arquivo salvo e íntegro. |
| 1 | Falha de rede, HTTP diferente de 200 e de 404, download incompleto, gzip inválido ou erro de escrita. |
| 2 | Argumento ou configuração inválidos, ano não publicado (404) ou destino já existente. |

## Código

- Um único `src/ETL/coletores/coleta_contratacoes.py`, organizado em:
  configuração, validação do ano, URL, destino, sessão HTTP, download com
  verificação e `main`.
- Saem `ocds.py`, `processa_ano`, `EscritorCSV`, as colunas e o mapa dos CSVs e a
  contagem do monitoramento.
- `.env.example` troca `OCP_COMPRAS_DOWNLOAD_URL` por `OCP_DOWNLOAD_URL`, com a
  URL da publicação 157. `requirements.txt` e `.gitattributes` não mudam.

## Testes

Sem acesso à OCP, com o servidor HTTP local que já existe nos testes:

- sucesso: código 0 e arquivo salvo idêntico ao servido;
- destino padrão (`outputs/contratacoes/{ano}/{AAAAMMDD}_{HHMMSS}_contratacoes_{ano}.jsonl.gz`);
- `--saida` relativo ao diretório atual, criando as pastas que faltam;
- destino existente (arquivo ou pasta) recusado com código 2 sem acessar a fonte;
- 404 com código 2, sem criar a pasta do destino, e 500 com código 1;
- `Content-Encoding: gzip` gravado sem decodificar;
- download truncado pelo `Content-Length`, com código 1;
- gzip com o tamanho certo e o CRC errado, com código 1;
- resposta 200 que não é gzip, com código 1;
- em todas as falhas: nenhum `.part` sobra e o destino não é criado;
- argumentos inválidos (`--ano` ausente, `26`, `abcd`, `２０２６` e as opções que
  saíram), com código 2;
- configuração inválida (URL sem esquema, timeout zero ou não numérico) com
  código 2, e variável vazia usando o padrão;
- os dois testes do wrapper Bash: chamado de outro diretório com `--saida`
  relativo, e argumento inválido.

## Documentação

O README é reescrito para o novo escopo: fonte (publicação 157, com todos os
itens, não só medicamentos), uso, destino, integridade, códigos de saída,
configuração e testes. Ele informa que o tratamento dos dados fica no
`medicamentos-fdcc`.

## Impacto no medicamentos-fdcc

O wrapper da task `coleta-contratacoes` passa `--anos`, `--uf` e `--esferas` e
espera uma pasta de saída. Ele deixa de funcionar com esta versão. O merge desta
mudança na `main` deste repositório deve esperar a task de lá ser adaptada: ela
passa a chamar o coletor uma vez por ano, com `--saida` apontando para o
arquivo, e descompacta cada `.jsonl.gz` ao lado dele. A seleção dos
medicamentos e dos compradores do DF virá depois, também no `medicamentos-fdcc`,
fora desta especificação.

## Fora do escopo

- Vários anos por execução.
- Validar o conteúdo das linhas (JSON, campos OCDS).
- Arquivo de metadados ou de checksum ao lado do `.jsonl.gz`; o SHA-256 fica no
  log.
- Reaproveitar um download anterior do mesmo ano.
