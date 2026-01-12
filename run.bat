@echo off
setlocal EnableDelayedExpansion

echo.
echo ========================================================
echo    Dakota Analytics - Energy Data Pipeline
echo ========================================================
echo.

:: Check if Docker is running
docker info >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Docker is not running. Please start Docker Desktop and try again.
    pause
    exit /b 1
)
echo [OK] Docker is running

:: Create directories
mkdir database 2>nul
mkdir ingestion\staging 2>nul

:: Check/Create .env file
if not exist ".env" (
    echo.
    echo No .env file found. Creating one...
    echo Get your free EIA API key at: https://www.eia.gov/opendata/register.php
    echo.
    set /p USER_API_KEY="Enter your EIA API Key (or press Enter for demo): "
    
    if "!USER_API_KEY!"=="" set "USER_API_KEY=DEMO_KEY"
    
    (
        echo EIA_API_KEY=!USER_API_KEY!
        echo API_URL=http://synthetic_api:8000
        echo DATABASE_PATH=/database/energy_data.duckdb
        echo DAGSTER_HOME=/app/dagster_home
        echo DBT_PROJECT_DIR=/dbt
        echo DBT_PROFILES_DIR=/dbt
        echo DBT_TARGET=dev
    ) > .env
    echo [OK] .env file created
)

:: Stop existing containers
echo.
echo Stopping any existing containers...
docker compose down --remove-orphans 2>nul

:: Build and run in detached mode
echo.
echo Building and starting containers...
echo (First run may take several minutes)
echo.
docker compose up -d --build

if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to start containers.
    pause
    exit /b 1
)

:: Wait for services to be ready
echo.
echo Waiting for services to initialize...
echo.

:: Wait for Synthetic API
echo Waiting for Synthetic API...
set /a counter=0
:wait_api
set /a counter+=1
if %counter% GTR 30 (
    echo [WARNING] Synthetic API may not be ready. Continuing anyway...
    goto :api_done
)
curl -s http://localhost:8000/health >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo   Attempt %counter%/30 - API not ready yet...
    timeout /t 2 /nobreak >nul
    goto :wait_api
)
echo [OK] Synthetic API is ready
:api_done

:: Wait for Dagster
echo Waiting for Dagster...
set /a counter=0
:wait_dagster
set /a counter+=1
if %counter% GTR 30 (
    echo [WARNING] Dagster may not be ready. Continuing anyway...
    goto :dagster_done
)
curl -s http://localhost:3000 >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo   Attempt %counter%/30 - Dagster not ready yet...
    timeout /t 2 /nobreak >nul
    goto :wait_dagster
)
echo [OK] Dagster is ready
:dagster_done

:: Run Tests FIRST
echo.
echo ========================================================
echo   Running Tests...
echo ========================================================
echo.

:: Run orchestration tests
echo [1/3] Orchestration Tests
echo ----------------------------------------
docker compose exec -T dagster_app pytest /app/tests/ -v --tb=short
echo.

:: Run dbt tests
echo [2/3] dbt Tests
echo ----------------------------------------
docker compose exec -T dagster_app pytest /dbt/tests/ -v --tb=short
echo.

:: Run ingestion tests
echo [3/3] Ingestion Tests
echo ----------------------------------------
docker compose exec -T dagster_app pytest /ingestion/tests/ -v --tb=short
echo.

echo [OK] Tests completed!

:: Materialize all assets AFTER tests pass
echo.
echo ========================================================
echo   Materializing Pipeline Assets...
echo ========================================================
echo.

timeout /t 3 /nobreak >nul

docker compose exec -T dagster_app dagster asset materialize -m dagster_project --select "*"
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [WARNING] Some assets may have failed. Check Dagster UI for details.
) else (
    echo.
    echo [OK] All assets materialized successfully!
)

:: Done
echo.
echo ========================================================
echo    SETUP COMPLETE!
echo ========================================================
echo.
echo    Synthetic API:  http://localhost:8000
echo    Dagster UI:     http://localhost:3000
echo    GraphQL:        http://localhost:8000/graphql
echo.
echo    Database: ./database/energy_data.duckdb
echo.
echo    To stop:    docker compose down
echo    To logs:    docker compose logs -f
echo    To restart: docker compose restart
echo.
echo ========================================================
echo.
pause
