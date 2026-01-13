FROM python:3.12-slim

WORKDIR /app

# Minimal OS deps (add only if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl wget bash \
 && rm -rf /var/lib/apt/lists/*

# If you use requirements.txt, this will install it.
# If you use pyproject/poetry instead, tell me and I'll switch this to poetry properly.
COPY requirements.txt* /app/
RUN if [ -f requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi

# Copy app code (dev data is excluded by .dockerignore)
COPY . /app

EXPOSE 8501
