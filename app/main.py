import aiofiles
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .dependencies import config


app = FastAPI(
    title="ObjectNat effects API",
    description="API for calculating effects for territory by ObjectNat library",
    version=config.get("APP_VERSION"),
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/status")
async def read_root():
    return {"status": "OK"}

@app.get("/logs")
async def read_logs():
    async with aiofiles.open(config.get("LOGS_FILE")) as logs_file:
        logs = await logs_file.read()
        return logs[-1:-10000]
        