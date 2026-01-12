# Dagster Orchestration Dockerfile
# NOTE: Build context is project root (not orchestration/)
FROM python:3.11-slim

# Install uv for fast dependency management and curl for healthchecks
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy and install dependencies (path relative to project root)
COPY orchestration/requirements.txt .
RUN uv pip install --system --no-cache -r requirements.txt

# Copy the dagster project code
COPY orchestration/dagster_project/ /app/dagster_project/

# Copy orchestration tests to /app/tests/
COPY orchestration/tests/ /app/tests/

# Copy the dbt project (including tests)
COPY dbt/ /dbt/

# Copy the ingestion module (including tests)
COPY ingestion/ /ingestion/

# Copy the database folder (for DuckDB)
COPY database/ /database/

# Copy the reports folder
COPY reports/ /reports/

# Install dbt packages
RUN cd /dbt && dbt deps

# Set Dagster home
ENV DAGSTER_HOME=/app/dagster_home

# Create necessary directories
RUN mkdir -p /app/dagster_home /ingestion/staging /app/tests

# Copy startup script
COPY orchestration/startup.sh /app/startup.sh
RUN chmod +x /app/startup.sh

EXPOSE 3000

# Use startup script as entrypoint
CMD ["/app/startup.sh"]
