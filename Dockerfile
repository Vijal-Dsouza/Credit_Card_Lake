FROM python:3.11-slim
WORKDIR /app
RUN pip install duckdb==0.10.0 dbt-core==1.7.0 dbt-duckdb==1.7.0 python-dotenv pandas pyarrow "protobuf>=4.0.0,<5.0.0"
COPY . /app
RUN chown -R 1000:1000 /app
USER 1000:1000
CMD ["python", "pipeline.py"]
