FROM python:3.12-slim-bookworm

# PySpark 4.2 requires Java 17 or newer.
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        libgomp1 \
        procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

# Include only the application and trained model, not the notebook or venv.
COPY apps.py model.pkl ./

CMD ["python", "apps.py"]
