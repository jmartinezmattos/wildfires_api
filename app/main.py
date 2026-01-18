from dotenv import load_dotenv
from fastapi import FastAPI
from app.routers import router
from app.db import db_client
from contextlib import asynccontextmanager

load_dotenv("config/.env")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Código que se ejecuta al iniciar la app
    await db_client.connect()
    yield
    # Código que se ejecuta al cerrar la app (opcional)
    if db_client.pool:
        db_client.pool.close()
        await db_client.pool.wait_closed()

app = FastAPI(title="wildfires-api", lifespan=lifespan)

app.include_router(router)
