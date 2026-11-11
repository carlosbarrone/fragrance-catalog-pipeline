FROM apache/airflow:3.2.1

WORKDIR /opt/airflow

USER root

COPY pyproject.toml uv.lock ./
COPY src/ ./src/
COPY README.md ./

RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    /root/.local/bin/uv pip install --system --editable . && \
    python -m pip install --no-cache-dir pymongo && \
    chown -R airflow:root /opt/airflow

USER airflow

ENV PYTHONPATH="/opt/airflow"