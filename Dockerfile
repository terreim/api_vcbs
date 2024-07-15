# syntax=docker/dockerfile:1

ARG PYTHON_VERSION=3.11.9
FROM python:${PYTHON_VERSION}-slim as base

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install dependencies
RUN apt-get update && apt-get install -y \
    chromium-driver \
    wget \
    libnss3 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libatspi2.0-0 \
    libxshmfence1 \
    libgtk-3-0 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-cache-dir -r requirements.txt

# Set environment variable for Playwright
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Install Playwright and browsers
RUN pip install playwright && mkdir -p /ms-playwright && playwright install
RUN pip install Flask["async"]

COPY . .

EXPOSE 8000

CMD ["gunicorn", "--bind", "0.0.0.0:8000", "api_loader:app", "--workers", "5", "--threads", "2", "--timeout", "120"]
