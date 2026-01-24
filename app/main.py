from dotenv import load_dotenv
from fastapi import FastAPI
from app.routers import router
from app.db import db_client
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware

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
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(router)
