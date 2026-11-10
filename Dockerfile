# Restatement Radar: one image for the dashboard (API + web app) and the workers.

# --- build the web app -------------------------------------------------------------------
FROM node:22.11-alpine AS web
WORKDIR /web
COPY radar/web/package.json radar/web/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY radar/web/ ./
RUN npm run build

# --- runtime ---------------------------------------------------------------------------
FROM python:3.13-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    RADAR_RAW_DIR=/data/raw
WORKDIR /app
COPY pyproject.toml ./
COPY radar ./radar
COPY --from=web /web/dist ./radar/web/dist
RUN pip install --no-cache-dir . && rm -rf /app/radar \
    && useradd --create-home radar && mkdir -p /data/raw && chown -R radar /data
USER radar
WORKDIR /home/radar
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/api/health')"
CMD ["radar", "serve", "--host", "0.0.0.0", "--port", "8000"]
