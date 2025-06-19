################################################################################
#  texera/deployment/chat-assistant-service.dockerfile
#
#  Build context:  texera  (¹ see build command below)
#  Service code:   core/chat-assistant-service/*
################################################################################

# ──────────────────────────────────────────────────────────────────────────────
# 1. Base image
# ──────────────────────────────────────────────────────────────────────────────
FROM python:3.12-slim

# ──────────────────────────────────────────────────────────────────────────────
# 2. Prepare working directory
# ──────────────────────────────────────────────────────────────────────────────
WORKDIR /app

# ──────────────────────────────────────────────────────────────────────────────
# 3. Install Python deps  (requirements.txt lives in service folder)
# ──────────────────────────────────────────────────────────────────────────────
COPY core/chat-assistant-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ──────────────────────────────────────────────────────────────────────────────
# 4. Copy the service source tree into the image
# ──────────────────────────────────────────────────────────────────────────────
COPY core/chat-assistant-service /app

# ──────────────────────────────────────────────────────────────────────────────
# 5. Add the restart-loop entrypoint (lives in deployment/)
# ──────────────────────────────────────────────────────────────────────────────
COPY core/chat-assistant-service/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

# ──────────────────────────────────────────────────────────────────────────────
# 6. Expose the port required by the spec
# ──────────────────────────────────────────────────────────────────────────────
EXPOSE 9095