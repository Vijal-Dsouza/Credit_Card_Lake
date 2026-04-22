FROM python:3.11-slim
WORKDIR /app
RUN pip install duckdb==0.10.0 dbt-core==1.7.0 dbt-duckdb==1.7.0 python-dotenv pandas pyarrow
COPY . /app
RUN chown -R 1000:1000 /app
USER 1000:1000
CMD ["python", "pipeline.py"]
