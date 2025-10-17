from pydantic import BaseModel

#{'name': 'Brazil', 'alpha2': 'BR', 'alpha3': 'BRA', 'count': 60, 'avg_score': 1.0}
class GeoEntity(BaseModel):
    name: str
    alpha2: str
    alpha3: str
    count: int
    avg_score: float

