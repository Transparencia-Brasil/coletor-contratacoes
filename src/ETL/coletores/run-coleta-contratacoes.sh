#!/bin/bash
# Executa o coletor de contratações OCDS (Compras.gov.br) filtrado por UF.
#
# Uso (de qualquer pasta):
#   bash tasks/coleta-contratacoes/src/run-coleta-contratacoes.sh 2025 2026
#   bash tasks/coleta-contratacoes/src/run-coleta-contratacoes.sh 2026 -- --esferas Distrital
#
# Os argumentos antes de "--" são os anos. Os que vêm depois são repassados ao
# script Python (ver `python coleta_contratacoes.py --help`).

set -euo pipefail

# Garante que o script rode a partir da raiz do repositório
cd "$(dirname "$0")/../../.." || { echo "Erro: não foi possível acessar a raiz do projeto."; exit 1; }

ANOS=()
while [ $# -gt 0 ] && [ "$1" != "--" ]; do
  ANOS+=("$1")
  shift
done
[ "${1:-}" = "--" ] && shift

if [ ${#ANOS[@]} -eq 0 ]; then
  echo "Erro: informe ao menos um ano. Ex.: $0 2026"
  exit 1
fi

# Usa o Python do .venv da raiz, se existir (Linux/macOS ou Windows/Git Bash)
if [ -x ".venv/bin/python" ]; then
  PYTHON=".venv/bin/python"
elif [ -x ".venv/Scripts/python.exe" ]; then
  PYTHON=".venv/Scripts/python.exe"
else
  PYTHON="python3"
fi

echo "INÍCIO: $(date +"%d-%b-%Y %H:%M:%S")"
echo "Python: $PYTHON"
echo "Anos: ${ANOS[*]}"
echo ""

"$PYTHON" tasks/coleta-contratacoes/src/coleta_contratacoes.py --anos "${ANOS[@]}" "$@"

echo ""
echo "FIM: $(date +"%d-%b-%Y %H:%M:%S")"
