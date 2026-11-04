# Coletor de contratações: somente download e verificação de integridade

Data: 2026-11-04. Status: aprovado em conversa, aguardando implementação.

## Objetivo

Baixar, para um ano informado, o arquivo `{ano}.jsonl.gz` da publicação 153 do
OCP Data Registry (Medicamentos Transparentes), verificar se ele chegou íntegro
e salvá-lo exatamente como veio. O coletor não filtra, não converte e não
reescreve dados.

Hoje o coletor baixa a publicação 157 (Compras.gov.br), filtra por UF e esfera e
gera CSVs (contratações, itens, adjudicações, monitoramento e erros). Esse
tratamento sai do coletor e não é migrado: o tratamento dos dados baixados fica
no `medicamentos-fdcc`, no script `tasks/mapeamento-ocds/src/mapeamento-compras-ocds.py`,
que será reescrito para receber o OCDS da publicação 153.

## Fonte

[Publicação 153 do OCP Data Registry](https://data.open-contracting.org/en/publication/153),
Medicamentos Transparentes, sob licença CC BY 4.0:

- É publicada pela própria Transparência Brasil e coletada pela OCP todo mês. Os
  processos vêm do PNCP, de todos os sistemas de compras.
- Contém só os itens de medicamentos (CATMAT classe 6505). Eles são selecionados
  por filtro de princípio ativo na descrição e associados a um item do CATMAT por
  um modelo de linguagem (LLM). É um dado derivado, não a fonte oficial.
- Já vem em OCDS, uma release compilada por linha.

A publicação 157 (Compras.gov.br, SEGES/MGI) segue o mesmo padrão de URL e
continua acessível pela variável `OCP_DOWNLOAD_URL`, caso seja preciso comparar
com a fonte oficial.

## Contexto verificado em 2026-11-04

- `GET https://data.open-contracting.org/en/publication/153/download?name=2026.jsonl.gz`
  responde `302` para
  `https://fastly.data.open-contracting.org/downloads/brazil_medicamentos_transparentes/4353/2026.jsonl.gz`.
  O número `4353` identifica a versão da publicação e muda a cada atualização.
- O CDN envia `Content-Length` (6.624.895 bytes em 2026),
  `Content-Type: application/gzip`, `Last-Modified` (01/11/2026) e `ETag`.
- Há arquivos de 2021 a 2026, um `full.jsonl.gz` com tudo (39,7 MB) e arquivos
  `2304` e `2400`, que devem vir de datas erradas nos dados. Como `--ano` aceita
  quatro dígitos, esses também podem ser baixados.
- Um ano não publicado (`2019`) responde `404`.
- A fonte não publica checksum. O `ETag` é formado pelo tamanho e pela data, não
  por um hash do conteúdo. A integridade é verificada pelo `Content-Length` e
  pelo CRC32 e pelo tamanho original gravados no próprio gzip.
- A publicação 157 se comporta igual: `302` para o CDN, `Content-Length`,
  `Last-Modified` e `404` para ano não publicado.

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
| `OCP_DOWNLOAD_URL` | `https://data.open-contracting.org/en/publication/153/download` | Endereço de download da publicação; o ano vai em `?name={ano}.jsonl.gz`. HTTP(S), com host, sem query nem fragmento. |
| `DOWNLOAD_TIMEOUT_SECONDS` | `60` | Segundos, por tentativa, para conectar e entre leituras da resposta. Número positivo e finito. |

Uma variável definida mas vazia usa o padrão. Um valor inválido encerra com
código 2 antes de acessar a fonte.

`OCP_DOWNLOAD_URL` substitui `OCP_COMPRAS_DOWNLOAD_URL`, cujo nome vinha da
publicação 157. A variável antiga deixa de ser lida.

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
5. Grava o corpo em blocos de 1 MiB em `{destino}.part`, na mesma pasta do
   destino, calculando o SHA-256 ao mesmo tempo.
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
  URL da publicação 153. `requirements.txt` e `.gitattributes` não mudam.

## Testes

Sem acesso à OCP, com o servidor HTTP local que já existe nos testes:

- sucesso: código 0 e arquivo salvo idêntico ao servido;
- destino padrão (`outputs/contratacoes/{ano}/{AAAAMMDD}_{HHMMSS}_contratacoes_{ano}.jsonl.gz`);
- `--saida` relativo ao diretório atual, criando as pastas que faltam;
- destino existente (arquivo ou pasta) recusado com código 2 sem acessar a fonte;
- 404 com código 2 e 500 com código 1;
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

O README é reescrito para o novo escopo: fonte (com o aviso de que a publicação
153 é um dado derivado, classificado com LLM), uso, destino, integridade, códigos
de saída, configuração e testes. Ele informa que o tratamento dos dados fica no
`medicamentos-fdcc`.

## Impacto no medicamentos-fdcc

O wrapper da task `coleta-contratacoes` passa `--anos`, `--uf` e `--esferas` e
espera uma pasta de saída. Ele deixa de funcionar com esta versão. O merge desta
mudança na `main` deste repositório deve esperar a task de lá ser adaptada: ela
passa a chamar o coletor uma vez por ano, com `--saida` apontando para o
arquivo. O tratamento sobre os arquivos baixados fica no
`mapeamento-compras-ocds.py` reescrito. As duas adaptações são trabalho do
`medicamentos-fdcc`, fora desta especificação.

## Fora do escopo

- Vários anos por execução.
- Validar o conteúdo das linhas (JSON, campos OCDS).
- Arquivo de metadados ou de checksum ao lado do `.jsonl.gz`; o SHA-256 fica no
  log.
- Reaproveitar um download anterior do mesmo ano.
