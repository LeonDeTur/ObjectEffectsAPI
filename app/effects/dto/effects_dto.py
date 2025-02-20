from typing import Optional

from pydantic import BaseModel, Field


class EffectsDTO(BaseModel):

    project_id: int = Field(..., examples=[72], description="Project ID")
    scenario_id: int = Field(..., examples=[192], description="Scenario ID")
    service_type_id: int = Field(..., examples=[7], description="Service type ID")
    year: Optional[int] = Field(
        default=2024,
        examples=[2024],
        description="Year for data retrieval")
    target_population: Optional[int] = Field(
        default=None,
        examples=[200],
        description="Target population for project territory"
    )
