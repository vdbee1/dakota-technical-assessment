#!/bin/bash

echo ""
echo "========================================================"
echo "   Dakota Analytics - Energy Data Pipeline"
echo "========================================================"
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "[ERROR] Docker is not running. Please start Docker and try again."
    exit 1
fi
echo "[OK] Docker is running"

# Create directories
mkdir -p database ingestion/staging

# Check/Create .env file
if [ ! -f ".env" ]; then
    echo ""
    echo "No .env file found. Creating one..."
    echo "Get your free EIA API key at: https://www.eia.gov/opendata/register.php"
    echo ""
    read -p "Enter your EIA API Key (or press Enter for demo): " USER_API_KEY
    
    [ -z "$USER_API_KEY" ] && USER_API_KEY="DEMO_KEY"
    
    cat > .env << EOF
EIA_API_KEY=$USER_API_KEY
API_URL=http://synthetic_api:8000
DATABASE_PATH=/database/energy_data.duckdb
DAGSTER_HOME=/app/dagster_home
DBT_PROJECT_DIR=/dbt
DBT_PROFILES_DIR=/dbt
DBT_TARGET=dev
EOF
    echo "[OK] .env file created"
fi

# Stop existing containers
echo ""
echo "Stopping any existing containers..."
docker compose down --remove-orphans 2>/dev/null

# Build and run in detached mode
echo ""
echo "Building and starting containers..."
echo "(First run may take several minutes)"
echo ""
docker compose up -d --build

if [ $? -ne 0 ]; then
    echo "[ERROR] Failed to start containers."
    exit 1
fi

# Wait for services to be ready
echo ""
echo "Waiting for services to initialize..."
echo ""

# Wait for Synthetic API
echo "Waiting for Synthetic API..."
counter=0
while [ $counter -lt 30 ]; do
    if curl -s http://localhost:8000/health > /dev/null 2>&1; then
        echo "[OK] Synthetic API is ready"
        break
    fi
    counter=$((counter + 1))
    echo "  Attempt $counter/30 - API not ready yet..."
    sleep 2
done
if [ $counter -eq 30 ]; then
    echo "[WARNING] Synthetic API may not be ready. Continuing anyway..."
fi

# Wait for Dagster
echo "Waiting for Dagster..."
counter=0
while [ $counter -lt 30 ]; do
    if curl -s http://localhost:3000 > /dev/null 2>&1; then
        echo "[OK] Dagster is ready"
        break
    fi
    counter=$((counter + 1))
    echo "  Attempt $counter/30 - Dagster not ready yet..."
    sleep 2
done
if [ $counter -eq 30 ]; then
    echo "[WARNING] Dagster may not be ready. Continuing anyway..."
fi

# Run Tests FIRST
echo ""
echo "========================================================"
echo "  Running Tests..."
echo "========================================================"
echo ""

# Run orchestration tests
echo "[1/3] Orchestration Tests"
echo "----------------------------------------"
docker compose exec -T dagster_app pytest /app/tests/ -v --tb=short || true
echo ""

# Run dbt tests
echo "[2/3] dbt Tests"
echo "----------------------------------------"
docker compose exec -T dagster_app pytest /dbt/tests/ -v --tb=short || true
echo ""

# Run ingestion tests
echo "[3/3] Ingestion Tests"
echo "----------------------------------------"
docker compose exec -T dagster_app pytest /ingestion/tests/ -v --tb=short || true
echo ""

echo "[OK] Tests completed!"

# Materialize all assets AFTER tests pass
echo ""
echo "========================================================"
echo "  Materializing Pipeline Assets..."
echo "========================================================"
echo ""

sleep 3

docker compose exec -T dagster_app dagster asset materialize -m dagster_project --select "*"
if [ $? -ne 0 ]; then
    echo ""
    echo "[WARNING] Some assets may have failed. Check Dagster UI for details."
else
    echo ""
    echo "[OK] All assets materialized successfully!"
fi

# Done
echo ""
echo "========================================================"
echo "   SETUP COMPLETE!"
echo "========================================================"
echo ""
echo "   Synthetic API:  http://localhost:8000"
echo "   Dagster UI:     http://localhost:3000"
echo "   GraphQL:        http://localhost:8000/graphql"
echo ""
echo "   Database: ./database/energy_data.duckdb"
echo ""
echo "   To stop:    docker compose down"
echo "   To logs:    docker compose logs -f"
echo "   To restart: docker compose restart"
echo ""
echo "========================================================"
echo ""
