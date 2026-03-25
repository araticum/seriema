FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

RUN mkdir -p /app/seriema
COPY . /app/seriema/
RUN test -f /app/seriema/__init__.py || touch /app/seriema/__init__.py

EXPOSE 8000

CMD ["gunicorn", "seriema.main:app", "-w", "2", "-k", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000"]
