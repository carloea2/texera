# 1. Base image
FROM python:3.12-slim

# 2. Set working dir
WORKDIR /app

# 3. Copy and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copy your app code
COPY . .

# 5. Expose the port uvicorn listens on
EXPOSE 8001

# 6. Use our entrypoint loop
ENTRYPOINT ["./entrypoint.sh"]