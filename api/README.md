Energy Enrichment API
Rationale
The Energy Enrichment API is a production-ready data service designed to fill the "information gap" in retail energy data. While the EIA provides commercial metrics, this service utilizes a Synthetic Engine to provide high-fidelity grid stability, environmental, and market volatility metrics. Built with FastAPI and Strawberry GraphQL, it ensures type-safety and allows orchestrators (like Dagster) to fetch only the specific enrichment fields needed for a given state.

Project Architecture
The service follows a modular design to separate the delivery layer from the data generation logic.

Code snippet

graph TD
    A[Dagster Orchestrator] -->|GraphQL Query| B(FastAPI Router)
    B --> C{Strawberry GraphQL}
    C --> D[Resolvers]
    D --> E[Synthetic Engine]
    E -->|Deterministic Generation| F[Enriched JSON Data]
Project Structure
Plaintext

api/
├── app/
│   ├── main.py            # FastAPI application & GraphQL Router
│   ├── graphql/
│   │   ├── schema.py      # GraphQL Schema definition
│   │   ├── gpl_types.py   # Strawberry Type definitions (Consumption, Pricing, Health)
│   │   └── resolvers.py   # Query logic & Error handling
│   ├── services/
│   │   └── synthetic.py   # Synthetic data generation logic
│   └── core/
│       └── config.py      # App configuration
├── pyproject.toml         # Dependency management (uv)
├── Dockerfile             # Container configuration
└── README.md              # Documentation
Getting Started
1. Running with Docker (Recommended)
This service is containerized to ensure it runs exactly the same in any environment.

Bash
cd api

# Build the image
docker build -t energy-api .

# Run the container
docker run -p 8000:8000 energy-api
The API will be available at: http://localhost:8000

2. Local Development
For local testing outside of Docker using the uv package manager:

Bash
cd api

# Install dependencies
uv pip install --system -r pyproject.toml

# Start the server
uvicorn app.main:app --reload
📡 API Usage & Documentation
GraphQL Playground
Navigate to http://localhost:8000/graphql to access the interactive IDE.

Sample Query
The API allows batching consumption and pricing metrics into a single request:

GraphQL

query {
  consumption(state: "CA", months: 1) {
    state
    period
    peak_share
    weather_index
    grid_stability_index
    renewable_share
  }
}

query {
  pricing(state: "CA", months: 1) {
    state
    period
    pricing_type
    congestion_premium
    fuel_cost_index
    volatility_score
    carbon_intensity
  }
}
Auto-Generated Documentation
Swagger UI (REST Health/Docs): http://localhost:8000/docs

GraphQL Schema: Accessible via the "Docs" tab in the Playground.