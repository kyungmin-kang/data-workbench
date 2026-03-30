from fastapi import APIRouter
from pydantic import BaseModel


router = APIRouter()


class DemoPricingResponse(BaseModel):
    pricing_score: float
    median_home_price: float
    rent_index: float


@router.get("/api/demo-pricing", response_model=DemoPricingResponse)
def demo_pricing() -> dict[str, float]:
    return {
        "pricing_score": 87.5,
        "median_home_price": 742000.0,
        "rent_index": 1.08,
    }
