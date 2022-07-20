FROM python:3.10-slim

WORKDIR /app

ENV PYTHONFAULTHANDLER=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1

# XXX: Remove this once we can upgrade to psycopg2 newer than 2.8.x
RUN apt-get update && apt-get install -y\
  build-essential \
  libpq-dev \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY migrations migrations
COPY brightsky brightsky
COPY setup.py .
COPY README.md .

RUN pip install .

ENTRYPOINT ["python", "-m", "brightsky"]
