# Polaris SQL Engine — Local Demo

No AWS account or external Polaris server required. Everything runs locally via Docker.

## Prerequisites
- Docker + Docker Compose
- Java 21+

## Steps

0. **Generate ANTLR grammar and build the fat jar** (from the repository root)
   ```bash
   ./gradlew :polaris-sql-engine:generateGrammarSource :polaris-sql-engine:shadowJar
   ```
   Then change into this directory:
   ```bash
   cd extensions/sql-engine/demo
   ```

1. **Start the environment**
   ```bash
   docker compose up -d
   ```

2. **Seed demo data** (run once)
   ```bash
   ./seed.sh
   ```
   This creates three Iceberg tables in MinIO:
   - `retail.orders` — 200 rows, partitioned by `region`
   - `retail.products` — 50 rows, unpartitioned
   - `retail.regions` — 3 rows, reference data

3. **Launch the SQL shell**
   ```bash
   java -jar ../build/libs/polaris-sql-engine-*-demo.jar demo.properties
   ```

## Example queries
```sql
SHOW TABLES IN retail

SELECT * FROM retail.orders WHERE region = 'us-east-1' LIMIT 10

SELECT * FROM retail.orders WHERE amount > 200 ORDER BY amount DESC LIMIT 5

DESCRIBE STATS retail.orders

DIAGNOSE TABLE retail.orders

EXPLAIN SELECT * FROM retail.orders WHERE region = 'us-east-1'

SHOW TABLE LOCATION retail.products

SHOW TABLE POLICIES retail.orders
```

## Tear down
```bash
docker compose down -v
```
