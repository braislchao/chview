FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy package definition first for layer caching
COPY pyproject.toml ./
COPY src/ ./src/
COPY demo/ ./demo/

# Install the package with demo extras (no dev deps needed at runtime)
RUN pip install --no-cache-dir -e ".[demo]"

# Streamlit config
COPY .streamlit/ ./.streamlit/

EXPOSE 8501

# Default: run the Streamlit app
CMD ["streamlit", "run", "src/chview/app.py", \
     "--server.address=0.0.0.0", \
     "--server.port=8501", \
     "--server.headless=true"]
