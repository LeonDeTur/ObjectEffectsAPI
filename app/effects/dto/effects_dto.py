from pydantic import BaseModel, Field


class EffectsDTO(BaseModel):

    project_id: int = Field(..., examples=[72], description="Project ID")
    scenario_id: int = Field(..., examples=[192], description="Scenario ID")
