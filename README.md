# Credit Card Transactions Lake

A credit card data lake built on the Medallion architecture (Bronze → Silver → Gold). Ingests daily transaction extract files, enforces data quality rules at each layer boundary, and surfaces clean aggregations queryable via DuckDB.
