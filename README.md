# Credit Card Transactions Lake

A credit card data lake built on the Medallion architecture (Bronze → Silver → Gold). Ingests daily transaction extract files, enforces data quality rules at each layer boundary, and surfaces clean aggregations queryable via DuckDB.

> **Host permission note:** Run `sudo chown -R 1000:1000 data/ source/` on the host before first run if you encounter permission errors writing to /data or /source.
