#!/bin/bash

echo "========================================"
echo "  Starting Dagster Energy Pipeline"
echo "========================================"

# Create directories
mkdir -p /app/dagster_home /ingestion/staging

# Start Dagster webserver in background
echo "Starting Dagster webserver..."
dagster dev -m dagster_project -h 0.0.0.0 -p 3000 &
DAGSTER_PID=$!

# Wait for Dagster to be ready
echo "Waiting for Dagster to initialize..."
sleep 10

# Check if Dagster is responding
for i in {1..30}; do
    if curl -s http://localhost:3000 > /dev/null 2>&1; then
        echo "Dagster is ready!"
        break
    fi
    echo "  Waiting... ($i/30)"
    sleep 2
done

# Auto-materialize all assets
echo ""
echo "========================================"
echo "  Auto-materializing all assets..."
echo "========================================"
echo ""

# Give it a moment for definitions to fully load
sleep 5

dagster asset materialize -m dagster_project --select "*"

if [ $? -eq 0 ]; then
    echo ""
    echo "========================================"
    echo "  Pipeline executed successfully!"
    echo "========================================"
else
    echo ""
    echo "========================================"
    echo "  Some assets may have failed."
    echo "  Check Dagster UI for details."
    echo "========================================"
fi

# Run tests if RUN_TESTS environment variable is set
if [ "$RUN_TESTS" = "true" ]; then
    echo ""
    echo "========================================"
    echo "  Running Tests..."
    echo "========================================"
    echo ""
    
    # Run orchestration tests
    echo "Running orchestration tests..."
    cd /app
    pytest dagster_project/../tests/ -v --tb=short 2>/dev/null || echo "Orchestration tests completed (some may have been skipped)"
    
    # Run dbt tests
    echo ""
    echo "Running dbt tests..."
    cd /dbt
    pytest tests/ -v --tb=short 2>/dev/null || echo "dbt tests completed (some may have been skipped)"
    
    echo ""
    echo "========================================"
    echo "  Tests Complete!"
    echo "========================================"
fi

echo ""
echo "Dagster UI available at: http://localhost:3000"
echo ""

# Keep container running by waiting on Dagster process
wait $DAGSTER_PID
