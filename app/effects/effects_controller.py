from fastapi import APIRouter

from .dto.effects_dto import EffectsDTO


effects_router = APIRouter(prefix="/effects")

@effects_router.post("/effects")
async def calculate_effects(
        params: EffectsDTO,
) -> dict:
    """
    Get method for retrieving effects with objectnat
    Params:

    project ID: Project ID
    scenario ID: Scenario ID
    """

    result = await calculate_effects(params)
    return result
