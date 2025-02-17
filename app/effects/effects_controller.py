from fastapi import APIRouter

from .dto.effects_dto import EffectsDTO
from .shemas.effects_base_schema import EffectsSchema


effects_router = APIRouter(prefix="/effects")

@effects_router.post("/effects")
async def calculate_effects(
        params: EffectsDTO,
) -> EffectsSchema:
    """
    Get method for retrieving effects with objectnat
    Params:

    project ID: Project ID
    scenario ID: Scenario ID
    """

    result = await calculate_effects(params)
    return result
