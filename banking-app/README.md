# Banking Application - Source Database

This directory contains the source PostgreSQL database for the banking application, including schema definitions and data generation tools.

## Overview

The banking application simulates a realistic banking system across six tables:

| Table | Holds | Notable column types |
|---|---|---|
| `users` | Customers, personal info, primary account | PII: `ssn`, `email`, `date_of_birth` |
| `transactions` | Transaction history | `numeric(15,2)` money columns |
| `accounts` | Normalised accounts, several per user | `real`, `double precision`, `timestamptz` |
| `merchants` | Transaction counterparties | `uuid`, `text[]`, `jsonb` |
| `loans` | Lending book | `interval`, `time`, `numeric(5,3)` |
| `audit_log` | Append-only action trail | `uuid`, `inet`, `bytea`, `jsonb` |

The last four carry Postgres types the first two do not. The Iceberg blueprint
maps a fixed list of types and silently falls back to `StringType` for anything
else, so these make that behaviour visible in the DAG generator's warnings
rather than theoretical. `audit_log` being append-only makes it the natural
table for testing `mode: append` against the blueprint's default `overwrite`.

## Quick Start

### 1. Start the Database

The source Postgres database is part of the main docker-compose setup in the root directory.

```bash
# From project root
cd ..
docker-compose -f docker-compose.airflow.yml up -d postgres-banking

# Check status
docker-compose -f docker-compose.airflow.yml ps postgres-banking
```

The database will be available at:
- **Host**: localhost
- **Port**: 5433
- **Database**: bankingdb
- **User**: bankinguser
- **Password**: bankingpass

### 2. Generate Data

```bash
# Install dependencies
pip install -r requirements.txt

# Generate sample data
python scripts/generate_banking_data.py --users 1000 --transactions 5

# With reproducible seed
python scripts/generate_banking_data.py --users 500 --transactions 8 --seed 42
```

### 3. Connect and Verify

```bash
# Connect to database
docker exec -it postgres-banking psql -U bankinguser -d bankingdb

# Or use psql directly
psql -h localhost -p 5433 -U bankinguser -d bankingdb
```

```sql
-- Check tables
\dt

-- View data
SELECT COUNT(*) FROM users;
SELECT COUNT(*) FROM transactions;

-- Sample queries
SELECT account_type, COUNT(*), AVG(balance)
FROM users
GROUP BY account_type;

SELECT transaction_type, COUNT(*), SUM(amount)
FROM transactions
GROUP BY transaction_type;
```

## Directory Structure

```
banking-app/
├── requirements.txt            # Python dependencies
├── init-scripts/               # Database initialization scripts
│   └── 02-init-banking-schema.sql   # All six tables
├── scripts/                    # Data generation tools
│   ├── generate_banking_data.py     # Populates all six tables
│   └── README.md
└── README.md                   # This file

Note: The Postgres database is defined in ../docker-compose.airflow.yml
```

## Database Schema

Full DDL lives in
[`init-scripts/02-init-banking-schema.sql`](init-scripts/02-init-banking-schema.sql) —
one file defining all six tables, their indexes, constraints, and column
comments. Postgres runs it automatically on a fresh volume, and the data
generator executes the same file when it creates tables, so the two can never
drift apart.

Inspect the live schema instead of trusting a copy:

```bash
docker exec -it postgres-banking psql -U bankinguser -d bankingdb

\dt                 -- list tables
\d+ loans           -- one table in detail
```

Relationships:

```
users ──< accounts ──< loans
  │                      │
  ├──< transactions      │
  ├──< loans ────────────┘
  └──< audit_log

merchants          (referenced by transactions.merchant as free text)
```

## Data Generator

The `generate_banking_data.py` script creates realistic banking data using:

- **Pydantic models** for type safety and validation
- **Faker library** for realistic names, addresses, emails, etc.
- **Smart logic** for transaction amounts based on account type and balance
- **Transaction categories** matching real-world patterns

### CLI Options

```bash
python scripts/generate_banking_data.py \
  --users 1000 \               # Number of users
  --transactions 5 \           # Avg transactions per user
  --accounts-per-user 2 \      # Max accounts per user
  --merchants 150 \            # Merchants to create
  --loan-ratio 0.4 \           # Fraction of users holding a loan
  --audit-events 2000 \        # Audit log rows
  --skip-extended \            # Only users and transactions
  --seed 42 \                  # Reproducible data
  --host localhost --port 5433 --database bankingdb \
  --user bankinguser --password bankingpass
```

Or from the project root: `make seed-data`.

The script recreates the schema by executing
`init-scripts/02-init-banking-schema.sql`, so there is a single definition of
the tables. Pass `--no-create-tables` to insert into an existing schema; the
extended tables are truncated first so reruns do not accumulate duplicates.

See `scripts/README.md` for detailed documentation.

## Common Operations

### Backup Database

```bash
# Backup to file
docker exec postgres-banking pg_dump -U bankinguser bankingdb > backup.sql

# Restore from file
docker exec -i postgres-banking psql -U bankinguser bankingdb < backup.sql
```

### Reset Database

```bash
# From project root
cd ..

# Stop and remove postgres-banking volume
docker-compose -f docker-compose.airflow.yml down
docker volume rm airflow-blue-print-project_postgres_banking_data

# Start fresh
docker-compose -f docker-compose.airflow.yml up -d postgres-banking

# Regenerate data
cd banking-app
python scripts/generate_banking_data.py --users 1000 --transactions 5
```

### View Logs

```bash
# From project root
docker-compose -f docker-compose.airflow.yml logs -f postgres-banking

# View last 100 lines
docker-compose -f docker-compose.airflow.yml logs --tail=100 postgres-banking
```

## Integration with Data Pipeline

This database serves as the source for the Airflow data pipeline that ingests data into Iceberg tables.

To run the complete pipeline:

1. **Start all services** (from project root):
   ```bash
   cd ..
   docker-compose -f docker-compose.airflow.yml up -d
   ```

2. **Generate banking data**:
   ```bash
   cd banking-app
   pip install -r requirements.txt
   python scripts/generate_banking_data.py --users 1000 --transactions 10
   ```

3. **Trigger a DAG** in the Airflow UI:
   - Open http://localhost:8080
   - Run `banking_users_to_iceberg` (or generate a new one with `make gen`)

## Troubleshooting

**Port already in use**
```bash
# Edit ../docker-compose.airflow.yml and change the port mapping
# Change "5433:5432" to "5434:5432" in postgres-banking service
```

**Permission denied**
```bash
# Make script executable
chmod +x scripts/generate_banking_data.py
```

**Connection refused**
```bash
# Check if container is running (from project root)
docker-compose -f docker-compose.airflow.yml ps postgres-banking

# Check logs
docker-compose -f docker-compose.airflow.yml logs postgres-banking
```

**Data not persisting**
```bash
# Check volume
docker volume ls | grep postgres_banking

# Volume is named: airflow-blue-print-project_postgres_banking_data
```

## Performance Tips

1. **Large datasets**: For >10,000 users, consider batching in the generator script
2. **Indexes**: Schema includes 13 indexes for optimal query performance
3. **Disk space**: Plan for ~1KB per user + ~0.5KB per transaction
4. **Memory**: Postgres container uses ~100MB base + working memory

## License

This project is for educational purposes.
