from dotenv import load_dotenv
from fastapi import FastAPI
from app.routers import router

load_dotenv("config/.env")

app = FastAPI(title="wildfires-api")

app.include_router(router)
