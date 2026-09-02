FROM python:3.12-slim


WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY sql/ ./sql/

ENV PYTHONPATH=/app/src

CMD ["python", "-m", "risk_feature_consumer", "--topic", "loan-applications"]