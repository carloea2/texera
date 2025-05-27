from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.endpoints.websocket_endpoint import router as websocket_router


def create_app() -> FastAPI:
    app = FastAPI()

    # Configure CORS; restrict origins as needed in production.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include the WebSocket endpoint router.
    app.include_router(websocket_router)
    return app
