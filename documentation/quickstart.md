# Quick Start Guide

## Prerequisites
- Docker Desktop installed and running
- EIA API key (free): https://www.eia.gov/opendata/register.php

## Run the Pipeline

**Windows:**
```bash
git clone https://github.com/YOUR_USERNAME/dakota-technical-assessment.git
cd dakota-technical-assessment
run.bat
```

**Mac/Linux:**
```bash
git clone https://github.com/YOUR_USERNAME/dakota-technical-assessment.git
cd dakota-technical-assessment
chmod +x run.sh
./run.sh
```

Enter your EIA API key when prompted (or press Enter for demo mode).

## Access
- **Dagster UI**: http://localhost:3000
- **Synthetic API**: http://localhost:8000
- **GraphQL Playground**: http://localhost:8000/graphql