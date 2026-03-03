#!/usr/bin/env bash
set -e

# Install Python deps
pip install -q -r backend/requirements.txt

# Download data from S3 (requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY)
python backend/sync_data.py

# Install Node deps & build frontend
cd frontend
npm install --silent
npm run build
cd ..

# Serve the built frontend from FastAPI as static files
# Copy build output next to the backend so uvicorn can serve it
cp -r frontend/dist backend/static 2>/dev/null || true

# Start the API server (serves both API and static frontend)
cd backend
uvicorn main:app --host 0.0.0.0 --port 8000
