from fastapi import FastAPI, Request, HTTPException, Depends
from strawberry.fastapi import GraphQLRouter
from app.graphql.schema import schema
from limits import parse
from limits.strategies import FixedWindowRateLimiter
from limits.storage import MemoryStorage
from typing import Dict

# 1. Setup Rate Limiter
storage = MemoryStorage()
limiter_engine = FixedWindowRateLimiter(storage)
limit = parse("5000/minute")

app = FastAPI(title="Energy Enrichment Service")

# 2. Bulletproof Dependency
def rate_limit_check(request: Request):
    client_ip = request.client.host if request.client else "127.0.0.1"
    if not limiter_engine.hit(limit, client_ip, "graphql_post"):
        raise HTTPException(
            status_code=429, 
            detail="Rate limit exceeded. Please wait a minute."
        )

# 3. Setup Router
graphql_router = GraphQLRouter(schema)

# 4. Attach Router with dependency
app.include_router(
    graphql_router,
    prefix="/graphql",
    dependencies=[Depends(rate_limit_check)]
)

# 5. CRITICAL: Health Check for Docker
@app.get("/health")
def health_check() -> Dict[str, str]:
    return {"status": "healthy"}

@app.get("/")
def root():
    return {"message": "API is online. Use /graphql for data or /health for status."}