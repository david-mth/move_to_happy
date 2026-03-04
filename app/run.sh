#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV="$PROJECT_ROOT/.venv/bin"
GCC_LIB="$(gcc -print-file-name=libstdc++.so.6 | xargs dirname)"

cd "$SCRIPT_DIR"

LD_LIBRARY_PATH="$GCC_LIB" "$VENV/python" backend/sync_data.py || true

cd frontend
npm install --silent
npm run build
cd ..

rm -rf backend/static
cp -r frontend/dist backend/static

cd backend
LD_LIBRARY_PATH="$GCC_LIB" "$VENV/uvicorn" main:app --host 0.0.0.0 --port 5000
