FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY static ./static
COPY specs ./specs
COPY runtime ./runtime
COPY data ./data

RUN pip install --no-cache-dir -e .

ENV PYTHONPATH=/app/src
ENV WORKBENCH_HOST=0.0.0.0
ENV WORKBENCH_PORT=8000

CMD ["python", "-m", "workbench.app"]
