FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS uv_builder
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
# Disable python downloads to use the one from the base image
ENV UV_PYTHON_DOWNLOADS=0
RUN apt-get update && apt-get install -y git

WORKDIR /app

COPY tools/src /app/src
COPY README.md /app/README.md
COPY LICENCE.md /app/LICENCE.md
COPY tools/pyproject.toml /app/pyproject.toml
COPY tools/uv.lock /app/uv.lock
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev


FROM python:3.12.11-slim-trixie AS production
# Create app user and group
RUN groupadd --gid 1000 app && \
    useradd --uid 1000 --gid app --shell /bin/bash --create-home app
# Add ps 
RUN apt-get update && \
    apt-get install -y procps && \
    rm -rf /var/lib/apt/lists/*
# Copy the application code from the builder stage
COPY --from=uv_builder --chown=app:app /app /app
# Copy the virtual environment with all dependencies from the builder stage
COPY --from=amazoncorretto:11.0.28-al2023-headless /usr/lib/jvm/java-11-amazon-corretto /usr/lib/jvm/java-11-amazon-corretto
# Copy certificates from the Corretto image
COPY --from=amazoncorretto:11.0.28-al2023-headless /etc/pki/ca-trust/extracted/java/cacerts /usr/lib/jvm/java-11-amazon-corretto/lib/security/cacerts



# Configure PATH to use the virtual environment's binaries
ENV PATH="/app/.venv/bin:$PATH"
# Set environment variables for PySpark and Hail locations
ENV JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto
ENV SPARK_HOME=/app/.venv/lib/python3.12/site-packages/pyspark
ENV HAIL_HOME=/app/.venv/lib/python3.12/site-packages/hail
CMD ["bin/bash"]