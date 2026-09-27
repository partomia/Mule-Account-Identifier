# Streamlit app only (no torch). Runs anywhere: reads parquet exports or CDW
# Impala, and calls the CAI model endpoint for what-ifs.
#
#   docker build -t mule-app .
#   docker run -p 8501:8501 -v $PWD/data/parquet:/app/data/parquet -v $PWD/models:/app/models mule-app
#   docker run -p 8501:8501 -e MULE_STORAGE_BACKEND=impala -e MULE_IMPALA_USER=... \
#       -e MULE_IMPALA_PASSWORD=... -e MULE_ENDPOINT_URL=... mule-app
FROM python:3.11-slim

WORKDIR /app
COPY requirements-app.txt .
RUN pip install --no-cache-dir -r requirements-app.txt

COPY mule/ mule/
COPY app/ app/
COPY config/ config/

ENV MULE_STORAGE_BACKEND=parquet \
    MULE_APP_HOST=0.0.0.0 \
    CDSW_APP_PORT=8501
EXPOSE 8501
CMD ["python", "app/run.py"]
