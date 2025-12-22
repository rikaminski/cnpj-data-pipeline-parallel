FROM python:3.12-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copy everything
COPY . .

# Install dependencies (no editable mode)
RUN uv pip install --system .

# Create temp directory
RUN mkdir -p /app/temp

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD python -c "print('ok')" || exit 1

CMD ["python", "main.py"]
