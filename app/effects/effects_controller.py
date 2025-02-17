from typing import Annotated

from fastapi import APIRouter, Depends


from .dto.effects_dto import EffectsDTO
from .shemas.effects_base_schema import EffectsSchema
from .effects_service import effects_service


effects_router = APIRouter(prefix="/effects")

@effects_router.post("/evaluate_provision", response_model=EffectsSchema)
async def calculate_effects(
        params: Annotated[EffectsDTO, Depends(EffectsDTO)],
) -> EffectsSchema:
    """
    Get method for retrieving effects with objectnat
    Params:

    project ID: Project ID
    scenario ID: Scenario ID
    """

    result = await effects_service.calculate_effects(params)
    return EffectsSchema(**result)
