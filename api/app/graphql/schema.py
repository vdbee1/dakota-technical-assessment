import strawberry
from strawberry.schema.config import StrawberryConfig
from .resolvers import Query

schema = strawberry.Schema(
    query=Query, 
    config=StrawberryConfig(auto_camel_case=False)
)