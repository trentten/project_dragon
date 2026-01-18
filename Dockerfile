FROM python:3.12-slim

WORKDIR /app

# Minimal OS deps (add only if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl wget bash \
 && rm -rf /var/lib/apt/lists/*

# Install requirements (if present)
COPY requirements.txt* /app/
RUN if [ -f requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi

# Copy app code (dev data is excluded by .dockerignore)
COPY . /app

# Install Project Dragon as a package
RUN pip install --no-cache-dir .

# Sanity check import
RUN python -c "import project_dragon; print('ok')"

EXPOSE 8501
