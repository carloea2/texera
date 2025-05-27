from fastapi import FastAPI
from app.endpoints.websocket_endpoint import router as websocket_router


def create_app() -> FastAPI:
    app = FastAPI()
    app.include_router(websocket_router)
    return app
