# Banking Data Generator

A Python script to generate realistic banking application data for testing and
development. Populates all six tables: `users`, `transactions`, `accounts`,
`merchants`, `loans`, and `audit_log`.

## Features

- **Pydantic Models**: Type-safe data models with validation
- **Realistic Data**: Uses Faker for names, addresses, emails, companies
- **Banking-Specific Logic**:
  - Multiple account types (checking, savings, credit, investment)
  - Realistic transaction patterns based on account type
  - Loan terms and rates that match the loan type
  - Credit scores, SSNs, and account numbers
- **Single schema source**: Creates tables by executing
  `../init-scripts/02-init-banking-schema.sql` rather than holding its own copy
- **Configurable**: Row counts per table via CLI arguments
- **Reproducible**: Optional seed for consistent data
- **Refreshes statistics**: Runs `ANALYZE` at the end, so row-count estimates
  are accurate immediately (the DAG generator reads them)

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Or install individually
pip install pydantic psycopg2-binary faker
```

## Usage

### Basic Usage

Generate 100 users with ~5 transactions each:

```bash
python generate_banking_data.py
```

### Custom Number of Records

```bash
# Generate 1000 users with average of 10 transactions each
python generate_banking_data.py --users 1000 --transactions 10
```

### Using Docker (Recommended)

Connect to the Postgres container running in Docker:

```bash
# Generate data in the source database
python generate_banking_data.py \
  --users 500 \
  --transactions 8 \
  --host localhost \
  --port 5433 \
  --database bankingdb \
  --user bankinguser \
  --password bankingpass
```

### Reproducible Data

Use a seed for consistent data generation:

```bash
python generate_banking_data.py --users 100 --transactions 5 --seed 42
```

### Skip Table Creation

If tables already exist:

```bash
python generate_banking_data.py --no-create-tables --users 500
```

## CLI Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--users` | 100 | Number of users to generate |
| `--transactions` | 5 | Average number of transactions per user |
| `--accounts-per-user` | 2 | Maximum accounts per user |
| `--merchants` | 150 | Merchants to generate |
| `--loan-ratio` | 0.4 | Fraction of users holding a loan |
| `--audit-events` | 2000 | Audit log rows |
| `--skip-extended` | False | Only populate users and transactions |
| `--seed` | None | Random seed for reproducible data |
| `--host` | localhost | PostgreSQL host |
| `--port` | 5433 | PostgreSQL port |
| `--database` | bankingdb | Database name |
| `--user` | bankinguser | Database username |
| `--password` | bankingpass | Database password |
| `--no-create-tables` | False | Insert into an existing schema (truncates the extended tables first) |

## Data Models

### User Model

```python
class User(BaseModel):
    user_id: Optional[int]
    first_name: str
    last_name: str
    email: str
    phone: str
    date_of_birth: datetime
    ssn: str  # Format: XXX-XX-XXXX
    address: str
    city: str
    state: str  # 2-letter code
    zip_code: str
    account_type: AccountType  # checking, savings, credit, investment
    account_number: str
    balance: Decimal
    credit_score: int  # 300-850
    is_active: bool
    created_at: datetime
    last_login: Optional[datetime]
```

### Transaction Model

```python
class Transaction(BaseModel):
    transaction_id: Optional[int]
    user_id: int
    transaction_type: TransactionType  # deposit, withdrawal, transfer, payment, fee, interest
    amount: Decimal
    balance_after: Decimal
    transaction_date: datetime
    description: str
    category: Optional[str]
    merchant: Optional[str]
    status: TransactionStatus  # pending, completed, failed, cancelled
    reference_number: str
    created_at: datetime
```

## Examples

### Generate Small Test Dataset

```bash
python generate_banking_data.py --users 50 --transactions 3 --seed 123
```

### Generate Large Production-Like Dataset

```bash
python generate_banking_data.py --users 10000 --transactions 15
```

### Generate and Verify

```bash
# Generate data
python generate_banking_data.py --users 1000 --transactions 5

# Connect to database and verify
psql -h localhost -p 5433 -U bankinguser -d bankingdb

# In psql:
SELECT COUNT(*) FROM users;
SELECT COUNT(*) FROM transactions;
SELECT account_type, COUNT(*) FROM users GROUP BY account_type;
SELECT transaction_type, COUNT(*) FROM transactions GROUP BY transaction_type;
```

## Database Schema

The script creates six tables by executing
[`../init-scripts/02-init-banking-schema.sql`](../init-scripts/02-init-banking-schema.sql),
which is the single definition of the schema:

| Table | Primary key | References |
|---|---|---|
| `users` | `user_id` | — |
| `transactions` | `transaction_id` | `users` |
| `accounts` | `account_id` | `users` |
| `merchants` | `merchant_id` | — |
| `loans` | `loan_id` | `users`, `accounts` |
| `audit_log` | `audit_id` | `users` |

Column definitions, constraints, and indexes are in that file rather than
duplicated here.

## Transaction Categories

The generator creates realistic transaction categories:

- **Payments**: Groceries, Utilities, Rent, Insurance, Gas, Dining, Entertainment, Healthcare, Shopping, Subscriptions
- **Deposits**: Salary, Bonus, Refund, Transfer In, Interest
- **Withdrawals**: ATM, Cash, Transfer Out
- **Transfers**: Internal Transfer, P2P Transfer
- **Fees**: Overdraft Fee, ATM Fee, Monthly Fee
- **Interest**: Interest Earned, Interest Charged

## Validation

Pydantic models provide automatic validation:

- Email format validation
- SSN format validation (XXX-XX-XXXX)
- Credit score range (300-850)
- Positive transaction amounts
- Non-negative account balances
- Enum validation for account types, transaction types, and statuses

## Tips

1. **Performance**: For large datasets (>10,000 users), consider increasing the commit frequency in the code
2. **Disk Space**: Plan for ~1KB per user and ~0.5KB per transaction
3. **Memory**: The script processes data in batches, so memory usage is minimal
4. **Network**: For remote databases, use `--host` to connect

## Troubleshooting

**Connection refused**
```bash
# Check if Postgres is running
docker ps | grep postgres-banking

# Check connection parameters
psql -h localhost -p 5433 -U bankinguser -d bankingdb
```

**Permission denied**
```bash
# Make script executable
chmod +x generate_banking_data.py
```

**Import errors**
```bash
# Install dependencies
pip install -r requirements.txt
```