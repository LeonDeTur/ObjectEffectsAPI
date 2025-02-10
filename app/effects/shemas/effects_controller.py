from fastapi import APIRouter


effects_router = APIRouter(prefix="/effects")

@effects_router.post("/effects")
async def calculate_effects():
    pass
