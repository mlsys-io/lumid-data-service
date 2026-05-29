FROM python:3.10-slim

WORKDIR /app

# Install deps first for layer caching. Slim by design — no asyncpg, redis,
# websockets, sse-starlette, or mcp: the injection service is a pure psycopg2
# write plane with no realtime / read / MCP surface. (ijson IS included: the
# optional adapter bridge imports the mounted loaders/lib, which needs it.)
COPY pyproject.toml /app/
RUN pip install --no-cache-dir \
        "fastapi>=0.111" \
        "uvicorn[standard]>=0.30" \
        "psycopg2-binary>=2.9" \
        "pydantic>=2.7" \
        "python-multipart>=0.0.9" \
        "slowapi>=0.1.9" \
        "cachetools>=5.3" \
        "scalar-fastapi>=1.0.3" \
        "httpx>=0.27" \
        "certifi>=2024" \
        "lxml>=5.0" \
        "pyyaml>=6.0" \
        "pyarrow>=15.0" \
        "zstandard>=0.22" \
        "ijson>=3.2"

# Copy the package. Build context is the repo root, so this lands the
# `injection/` package at /app/injection and `import injection.server` works.
COPY . /app

ENV PYTHONUNBUFFERED=1
# When a loaders/ tree is bind-mounted at /app/loaders (+ /app/CLAUDE.md),
# adapter mode activates. Set FINAI_ROOT explicitly so resolution is
# unambiguous; harmless when the mount is absent (adapter mode just 503s).
ENV FINAI_ROOT=/app
EXPOSE 8089

CMD ["uvicorn", "injection.server:app", "--host", "0.0.0.0", "--port", "8089"]
