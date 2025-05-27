import logging
import uvicorn
from app.create_app import create_app

logging.basicConfig(level=logging.INFO)
app = create_app()

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8001, reload=True)
