#!/usr/bin/env python3
"""
Banking application data generator.

Populates all six source tables: users, transactions, accounts, merchants,
loans and audit_log. Uses Pydantic for validation and Faker for realistic values.

The later four tables carry Postgres types the first two do not - uuid, jsonb,
text[], interval, time, inet, bytea - none of which the Iceberg blueprint maps.
They land as StringType, which the DAG generator surfaces as a warning.
"""

import argparse
import json
import random
import sys
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Optional

import psycopg2
from faker import Faker
from pydantic import BaseModel, Field, field_validator


# Enums for categorical data
class AccountType(str, Enum):
    """Types of bank accounts."""

    CHECKING = "checking"
    SAVINGS = "savings"
    CREDIT = "credit"
    INVESTMENT = "investment"


class TransactionType(str, Enum):
    """Types of transactions."""

    DEPOSIT = "deposit"
    WITHDRAWAL = "withdrawal"
    TRANSFER = "transfer"
    PAYMENT = "payment"
    FEE = "fee"
    INTEREST = "interest"


class TransactionStatus(str, Enum):
    """Transaction status."""

    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


# Vocabulary for the extended tables. Plain lists rather than enums, since
# these are only ever used to pick a random value.
ACCOUNT_STATUS = ["active", "active", "active", "frozen", "dormant", "closed"]
LOAN_TYPES = ["mortgage", "auto", "personal", "student", "credit_line"]
LOAN_STATUS = ["active", "active", "active", "paid_off", "delinquent", "defaulted"]
CURRENCIES = ["USD", "USD", "USD", "EUR", "GBP"]
MERCHANT_CATEGORIES = [
    "groceries", "restaurants", "travel", "utilities", "healthcare",
    "entertainment", "retail", "fuel", "insurance", "education",
]
AUDIT_ACTIONS = ["create", "read", "update", "delete", "login", "logout", "failed_login"]
ENTITY_TYPES = ["user", "account", "transaction", "loan", "merchant"]

# Term in months, typical rate, and principal range per loan type, so generated
# rows look plausible rather than uniformly random.
LOAN_SHAPES = {
    "mortgage":    (360, Decimal("5.5"),  200_000, 800_000),
    "auto":        (60,  Decimal("7.2"),  15_000,  60_000),
    "personal":    (36,  Decimal("11.9"), 3_000,   35_000),
    "student":     (120, Decimal("6.1"),  10_000,  120_000),
    "credit_line": (24,  Decimal("17.5"), 1_000,   25_000),
}


# Pydantic Models
class User(BaseModel):
    """Banking user model."""

    user_id: Optional[int] = None
    first_name: str = Field(..., min_length=1, max_length=50)
    last_name: str = Field(..., min_length=1, max_length=50)
    email: str = Field(..., max_length=100)
    phone: str = Field(..., max_length=20)
    date_of_birth: datetime
    ssn: str = Field(..., min_length=11, max_length=11)  # Format: XXX-XX-XXXX
    address: str = Field(..., max_length=200)
    city: str = Field(..., max_length=100)
    state: str = Field(..., min_length=2, max_length=2)
    zip_code: str = Field(..., max_length=10)
    account_type: AccountType
    account_number: str = Field(..., min_length=10, max_length=20)
    balance: Decimal = Field(ge=0)
    credit_score: int = Field(ge=300, le=850)
    is_active: bool = True
    created_at: datetime = Field(default_factory=datetime.now)
    last_login: Optional[datetime] = None

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: str) -> str:
        """Validate email format."""
        if "@" not in v:
            raise ValueError("Invalid email format")
        return v.lower()

    @field_validator("ssn")
    @classmethod
    def validate_ssn(cls, v: str) -> str:
        """Validate SSN format."""
        if not v.count("-") == 2:
            raise ValueError("SSN must be in format XXX-XX-XXXX")
        return v

    class Config:
        use_enum_values = True


class Transaction(BaseModel):
    """Banking transaction model."""

    transaction_id: Optional[int] = None
    user_id: int
    transaction_type: TransactionType
    amount: Decimal = Field(gt=0)
    balance_after: Decimal = Field(ge=0)
    transaction_date: datetime = Field(default_factory=datetime.now)
    description: str = Field(..., max_length=200)
    category: Optional[str] = Field(None, max_length=50)
    merchant: Optional[str] = Field(None, max_length=100)
    status: TransactionStatus = TransactionStatus.COMPLETED
    reference_number: str = Field(..., max_length=50)
    created_at: datetime = Field(default_factory=datetime.now)

    @field_validator("amount")
    @classmethod
    def validate_amount(cls, v: Decimal) -> Decimal:
        """Validate transaction amount."""
        if v <= 0:
            raise ValueError("Amount must be greater than 0")
        return round(v, 2)

    class Config:
        use_enum_values = True


# Data Generator
class BankingDataGenerator:
    """Generate realistic banking data."""

    def __init__(self, seed: Optional[int] = None):
        """Initialize the generator with optional seed for reproducibility."""
        self.fake = Faker()
        if seed:
            Faker.seed(seed)
            random.seed(seed)

        # Transaction categories
        self.categories = {
            TransactionType.PAYMENT: [
                "Groceries",
                "Utilities",
                "Rent",
                "Insurance",
                "Gas",
                "Dining",
                "Entertainment",
                "Healthcare",
                "Shopping",
                "Subscriptions",
            ],
            TransactionType.DEPOSIT: [
                "Salary",
                "Bonus",
                "Refund",
                "Transfer In",
                "Interest",
            ],
            TransactionType.WITHDRAWAL: ["ATM", "Cash", "Transfer Out"],
            TransactionType.TRANSFER: ["Internal Transfer", "P2P Transfer"],
            TransactionType.FEE: ["Overdraft Fee", "ATM Fee", "Monthly Fee"],
            TransactionType.INTEREST: ["Interest Earned", "Interest Charged"],
        }

    def generate_user(self) -> User:
        """Generate a single realistic user."""
        account_type = random.choice(list(AccountType))

        # Generate realistic balance based on account type
        if account_type == AccountType.CHECKING:
            balance = Decimal(random.uniform(100, 50000))
        elif account_type == AccountType.SAVINGS:
            balance = Decimal(random.uniform(1000, 100000))
        elif account_type == AccountType.CREDIT:
            balance = Decimal(random.uniform(0, 25000))
        else:  # INVESTMENT
            balance = Decimal(random.uniform(5000, 500000))

        # Generate date of birth (18-80 years old)
        dob = self.fake.date_of_birth(minimum_age=18, maximum_age=80)

        # Last login within the last 30 days for active users
        last_login = datetime.now() - timedelta(days=random.randint(0, 30))

        return User(
            first_name=self.fake.first_name(),
            last_name=self.fake.last_name(),
            email=self.fake.email(),
            phone=self.fake.phone_number()[:20],
            date_of_birth=dob,
            ssn=self.fake.ssn(),
            address=self.fake.street_address(),
            city=self.fake.city(),
            state=self.fake.state_abbr(),
            zip_code=self.fake.zipcode(),
            account_type=account_type,
            account_number=self.fake.bban()[:20],
            balance=round(balance, 2),
            credit_score=random.randint(300, 850),
            is_active=random.random() > 0.05,  # 95% active
            created_at=datetime.now() - timedelta(days=random.randint(30, 3650)),
            last_login=last_login if random.random() > 0.1 else None,
        )

    def generate_transaction(
        self, user_id: int, current_balance: Decimal
    ) -> Transaction:
        """Generate a single realistic transaction for a user."""
        transaction_type = random.choice(list(TransactionType))

        # Generate amount based on transaction type
        if transaction_type == TransactionType.DEPOSIT:
            amount = Decimal(random.uniform(100, 5000))
            balance_after = current_balance + amount
        elif transaction_type == TransactionType.WITHDRAWAL:
            max_withdrawal = min(float(current_balance) * 0.5, 1000)
            amount = Decimal(random.uniform(20, max_withdrawal)) if max_withdrawal > 20 else Decimal(20)
            balance_after = current_balance - amount
        elif transaction_type == TransactionType.PAYMENT:
            max_payment = min(float(current_balance) * 0.3, 500)
            amount = Decimal(random.uniform(10, max_payment)) if max_payment > 10 else Decimal(10)
            balance_after = current_balance - amount
        elif transaction_type == TransactionType.TRANSFER:
            max_transfer = min(float(current_balance) * 0.4, 2000)
            amount = Decimal(random.uniform(50, max_transfer)) if max_transfer > 50 else Decimal(50)
            balance_after = current_balance - amount
        elif transaction_type == TransactionType.FEE:
            amount = Decimal(random.uniform(5, 35))
            balance_after = current_balance - amount
        else:  # INTEREST
            amount = Decimal(float(current_balance) * random.uniform(0.0001, 0.002))
            balance_after = current_balance + amount

        # Ensure balance doesn't go negative
        balance_after = max(balance_after, Decimal(0))

        # Pick category
        category = random.choice(self.categories[transaction_type])

        # Generate merchant for payment transactions
        merchant = self.fake.company() if transaction_type == TransactionType.PAYMENT else None

        # Random status (mostly completed)
        status_weights = [0.02, 0.95, 0.02, 0.01]  # pending, completed, failed, cancelled
        status = random.choices(list(TransactionStatus), weights=status_weights)[0]

        # Transaction date within the last 90 days
        transaction_date = datetime.now() - timedelta(
            days=random.randint(0, 90),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )

        return Transaction(
            user_id=user_id,
            transaction_type=transaction_type,
            amount=round(amount, 2),
            balance_after=round(balance_after, 2),
            transaction_date=transaction_date,
            description=f"{transaction_type.value.title()}: {category}",
            category=category,
            merchant=merchant,
            status=status,
            reference_number=self.fake.uuid4()[:50],
            created_at=transaction_date,
        )


# Database Operations
class DatabaseManager:
    """Manage database operations."""

    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        """Initialize database connection."""
        self.conn = psycopg2.connect(
            host=host, port=port, database=database, user=user, password=password
        )
        self.cursor = self.conn.cursor()

    def create_tables(self):
        """Create every table by running the project's schema script.

        The DDL lives in init-scripts/02-init-banking-schema.sql, which Postgres
        also runs on a fresh volume. Executing that file rather than repeating
        the statements here keeps one definition of the schema.
        """
        schema = (
            Path(__file__).resolve().parents[1]
            / "init-scripts"
            / "02-init-banking-schema.sql"
        )
        if not schema.is_file():
            raise FileNotFoundError(f"Schema file not found: {schema}")

        self.cursor.execute(schema.read_text())
        self.conn.commit()
        print("✓ Tables created successfully")

    def truncate_extended(self):
        """Empty the four extended tables, leaving users and transactions."""
        self.cursor.execute(
            "TRUNCATE audit_log, loans, merchants, accounts RESTART IDENTITY CASCADE;"
        )
        self.conn.commit()
        print("✓ Cleared extended tables")

    def insert_user(self, user: User) -> int:
        """Insert a user and return the user_id."""
        self.cursor.execute(
            """
            INSERT INTO users (
                first_name, last_name, email, phone, date_of_birth, ssn,
                address, city, state, zip_code, account_type, account_number,
                balance, credit_score, is_active, created_at, last_login
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) RETURNING user_id;
        """,
            (
                user.first_name,
                user.last_name,
                user.email,
                user.phone,
                user.date_of_birth,
                user.ssn,
                user.address,
                user.city,
                user.state,
                user.zip_code,
                user.account_type,
                user.account_number,
                user.balance,
                user.credit_score,
                user.is_active,
                user.created_at,
                user.last_login,
            ),
        )
        user_id = self.cursor.fetchone()[0]
        return user_id

    def insert_transaction(self, transaction: Transaction):
        """Insert a transaction."""
        self.cursor.execute(
            """
            INSERT INTO transactions (
                user_id, transaction_type, amount, balance_after, transaction_date,
                description, category, merchant, status, reference_number, created_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            );
        """,
            (
                transaction.user_id,
                transaction.transaction_type,
                transaction.amount,
                transaction.balance_after,
                transaction.transaction_date,
                transaction.description,
                transaction.category,
                transaction.merchant,
                transaction.status,
                transaction.reference_number,
                transaction.created_at,
            ),
        )

    # ------------------------------------------------------------------
    # Extended tables. These take the already-generated user ids, so they run
    # after users exist; accounts and loans reference them by foreign key.
    # ------------------------------------------------------------------

    def generate_accounts(self, fake: Faker, user_ids: list[int], per_user: int) -> list[int]:
        """One to `per_user` accounts per user. Returns the new account ids."""
        account_ids: list[int] = []
        for user_id in user_ids:
            for index in range(random.randint(1, max(1, per_user))):
                opened = fake.date_time_between(start_date="-6y", end_date="-30d")
                status = random.choice(ACCOUNT_STATUS)
                balance = Decimal(random.randrange(0, 25_000_000)) / 100
                self.cursor.execute(
                    """
                    INSERT INTO accounts (
                        user_id, account_number, account_type, balance,
                        available_balance, interest_rate, overdraft_limit,
                        currency, is_primary, opened_at, closed_at,
                        last_activity_at, status
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING account_id;
                    """,
                    (
                        user_id,
                        f"ACC{random.randrange(10**11, 10**12)}",
                        random.choice([t.value for t in AccountType]),
                        balance,
                        # Available balance trails the ledger by pending items.
                        max(Decimal("0.00"), balance - Decimal(random.randrange(0, 50_000)) / 100),
                        round(random.uniform(0.01, 5.5), 3),
                        random.choice([0, 0, 0, 500, 1000, 2500]),
                        random.choice(CURRENCIES),
                        index == 0,
                        opened,
                        fake.date_time_between(start_date=opened) if status == "closed" else None,
                        fake.date_time_between(start_date=opened),
                        status,
                    ),
                )
                account_ids.append(self.cursor.fetchone()[0])
        return account_ids

    def generate_merchants(self, fake: Faker, count: int) -> None:
        """Counterparties. Exercises uuid, text[] and jsonb."""
        for _ in range(count):
            name = fake.company()
            self.cursor.execute(
                """
                INSERT INTO merchants (
                    merchant_uuid, name, legal_name, category, mcc_code,
                    country, city, website, tags, metadata, risk_score,
                    is_active, onboarded_on
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                """,
                (
                    str(uuid.uuid4()),
                    name,
                    f"{name} {random.choice(['Inc.', 'LLC', 'Ltd.', 'GmbH'])}",
                    random.choice(MERCHANT_CATEGORIES),
                    f"{random.randrange(1000, 9999)}",
                    fake.country_code(),
                    fake.city(),
                    f"https://{name.lower().replace(' ', '').replace(',', '')}.example.com",
                    random.sample(
                        ["online", "retail", "franchise", "seasonal", "high-volume"],
                        k=random.randint(1, 3),
                    ),
                    json.dumps({
                        "settlement_days": random.randint(1, 5),
                        "chargeback_rate": round(random.uniform(0, 0.05), 4),
                        "preferred": random.choice([True, False]),
                    }),
                    random.randint(0, 100),
                    random.random() > 0.1,
                    fake.date_between(start_date="-5y"),
                ),
            )

    def generate_loans(
        self, fake: Faker, user_ids: list[int], account_ids: list[int], ratio: float
    ) -> int:
        """Lending book for a fraction of users. Exercises interval and time."""
        borrowers = random.sample(user_ids, k=max(1, int(len(user_ids) * ratio)))
        for user_id in borrowers:
            loan_type = random.choice(LOAN_TYPES)
            term, base_rate, low, high = LOAN_SHAPES[loan_type]
            principal = Decimal(random.randrange(low, high))
            status = random.choice(LOAN_STATUS)
            outstanding = Decimal("0.00") if status == "paid_off" else (
                principal * Decimal(str(round(random.uniform(0.05, 1.0), 4)))
            ).quantize(Decimal("0.01"))
            origination = fake.date_between(start_date="-8y", end_date="-60d")
            rate = base_rate + Decimal(str(round(random.uniform(-1.5, 3.0), 3)))

            self.cursor.execute(
                """
                INSERT INTO loans (
                    user_id, account_id, loan_type, principal_amount,
                    outstanding_balance, interest_rate, term_months,
                    monthly_payment, grace_period, payment_due_time,
                    origination_date, maturity_date, next_payment_date,
                    status, collateral_desc
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                """,
                (
                    user_id,
                    random.choice(account_ids) if account_ids and random.random() > 0.3 else None,
                    loan_type,
                    principal,
                    outstanding,
                    max(Decimal("0.100"), rate),
                    term,
                    (principal / term * Decimal("1.15")).quantize(Decimal("0.01")),
                    timedelta(days=random.choice([0, 15, 30, 60])),
                    f"{random.randint(0, 23):02d}:00:00",
                    origination,
                    origination + timedelta(days=term * 30),
                    None if status == "paid_off"
                    else fake.date_between(start_date="today", end_date="+45d"),
                    status,
                    fake.sentence(nb_words=6) if loan_type in {"mortgage", "auto"} else None,
                ),
            )
        return len(borrowers)

    def generate_audit_log(self, fake: Faker, user_ids: list[int], count: int) -> None:
        """Append-only trail. Exercises uuid, inet, bytea and jsonb."""
        for _ in range(count):
            action = random.choice(AUDIT_ACTIONS)
            self.cursor.execute(
                """
                INSERT INTO audit_log (
                    event_uuid, user_id, entity_type, entity_id, action,
                    changed_fields, ip_address, user_agent, session_token,
                    occurred_at, duration_ms, succeeded
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                """,
                (
                    str(uuid.uuid4()),
                    random.choice(user_ids) if random.random() > 0.05 else None,
                    random.choice(ENTITY_TYPES),
                    random.randrange(1, 10_000),
                    action,
                    json.dumps({"field": "balance", "from": "100.00", "to": "250.00"})
                    if action == "update" else None,
                    fake.ipv4() if random.random() > 0.15 else fake.ipv6(),
                    fake.user_agent(),
                    psycopg2.Binary(uuid.uuid4().bytes),
                    fake.date_time_between(start_date="-1y"),
                    random.randrange(1, 4000),
                    action != "failed_login" and random.random() > 0.02,
                ),
            )

    def analyze(self):
        """Refresh planner statistics.

        Freshly created tables have no statistics, so pg_class.reltuples reads
        -1 and anything relying on it - the DAG generator's row estimates -
        reports "unknown" until autovacuum eventually runs.
        """
        self.cursor.execute("ANALYZE;")
        self.conn.commit()
        print("✓ Statistics updated")

    def commit(self):
        """Commit the transaction."""
        self.conn.commit()

    def close(self):
        """Close database connection."""
        self.cursor.close()
        self.conn.close()


# Main function
def main():
    """Main function to generate banking data."""
    parser = argparse.ArgumentParser(
        description="Generate realistic banking data for all six source tables.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 1000 users with 5 transactions each, plus the extended tables
  python generate_banking_data.py --users 1000 --transactions 5

  # Only users and transactions
  python generate_banking_data.py --users 100 --skip-extended

  # Generate with custom database connection
  python generate_banking_data.py --users 500 --transactions 10 \\
    --host localhost --port 5433 --database bankingdb

  # Use seed for reproducible data
  python generate_banking_data.py --users 100 --transactions 3 --seed 42
        """,
    )

    parser.add_argument(
        "--users",
        type=int,
        default=100,
        help="Number of users to generate (default: 100)",
    )
    parser.add_argument(
        "--transactions",
        type=int,
        default=5,
        help="Average number of transactions per user (default: 5)",
    )
    parser.add_argument(
        "--accounts-per-user",
        type=int,
        default=2,
        help="Maximum accounts per user (default: 2)",
    )
    parser.add_argument(
        "--merchants", type=int, default=150, help="Merchants to generate (default: 150)"
    )
    parser.add_argument(
        "--loan-ratio",
        type=float,
        default=0.4,
        help="Fraction of users holding a loan (default: 0.4)",
    )
    parser.add_argument(
        "--audit-events", type=int, default=2000, help="Audit log rows (default: 2000)"
    )
    parser.add_argument(
        "--skip-extended",
        action="store_true",
        help="Only populate users and transactions.",
    )
    parser.add_argument(
        "--seed", type=int, default=None, help="Random seed for reproducible data"
    )
    parser.add_argument(
        "--host",
        type=str,
        default="localhost",
        help="PostgreSQL host (default: localhost)",
    )
    parser.add_argument(
        "--port", type=int, default=5433, help="PostgreSQL port (default: 5433)"
    )
    parser.add_argument(
        "--database",
        type=str,
        default="bankingdb",
        help="Database name (default: bankingdb)",
    )
    parser.add_argument(
        "--user",
        type=str,
        default="bankinguser",
        help="Database user (default: bankinguser)",
    )
    parser.add_argument(
        "--password",
        type=str,
        default="bankingpass",
        help="Database password (default: bankingpass)",
    )
    parser.add_argument(
        "--no-create-tables",
        action="store_true",
        help="Skip table creation (tables must already exist)",
    )

    args = parser.parse_args()

    try:
        # Initialize generator
        print(f"Initializing data generator{' with seed ' + str(args.seed) if args.seed else ''}...")
        generator = BankingDataGenerator(seed=args.seed)

        # Connect to database
        print(f"Connecting to database {args.database}@{args.host}:{args.port}...")
        db = DatabaseManager(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=args.password,
        )

        # Create tables
        if not args.no_create_tables:
            print("Creating tables...")
            db.create_tables()

        # Generate users
        print(f"\nGenerating {args.users} users...")
        user_ids = []
        user_balances = {}

        for i in range(args.users):
            user = generator.generate_user()
            user_id = db.insert_user(user)
            user_ids.append(user_id)
            user_balances[user_id] = user.balance

            if (i + 1) % 100 == 0:
                print(f"  Generated {i + 1}/{args.users} users...")
                db.commit()

        db.commit()
        print(f"✓ Generated {args.users} users")

        # Generate transactions
        total_transactions = 0
        print(f"\nGenerating ~{args.users * args.transactions} transactions...")

        for i, user_id in enumerate(user_ids):
            # Vary number of transactions per user
            num_transactions = random.randint(
                max(1, args.transactions - 2), args.transactions + 3
            )

            current_balance = user_balances[user_id]

            for _ in range(num_transactions):
                transaction = generator.generate_transaction(user_id, current_balance)
                db.insert_transaction(transaction)
                current_balance = transaction.balance_after
                total_transactions += 1

            if (i + 1) % 100 == 0:
                print(f"  Generated transactions for {i + 1}/{args.users} users...")
                db.commit()

        db.commit()
        print(f"✓ Generated {total_transactions} transactions")

        # Extended tables. Skipped entirely with --skip-extended; otherwise
        # cleared first so re-running does not pile up duplicates. (users and
        # transactions do not need this - create_tables already dropped them.)
        counts: dict[str, int] = {}
        if not args.skip_extended:
            if args.no_create_tables:
                db.truncate_extended()

            print("\nGenerating extended tables...")
            account_ids = db.generate_accounts(
                generator.fake, user_ids, args.accounts_per_user
            )
            counts["accounts"] = len(account_ids)
            print(f"✓ accounts   {len(account_ids)}")

            db.generate_merchants(generator.fake, args.merchants)
            counts["merchants"] = args.merchants
            print(f"✓ merchants  {args.merchants}")

            counts["loans"] = db.generate_loans(
                generator.fake, user_ids, account_ids, args.loan_ratio
            )
            print(f"✓ loans      {counts['loans']}")

            db.generate_audit_log(generator.fake, user_ids, args.audit_events)
            counts["audit_log"] = args.audit_events
            print(f"✓ audit_log  {args.audit_events}")

            db.commit()

        db.analyze()

        # Print summary
        print("\n" + "=" * 60)
        print("DATA GENERATION SUMMARY")
        print("=" * 60)
        print(f"Users created:        {args.users}")
        print(f"Transactions created: {total_transactions}")
        print(f"Avg transactions/user: {total_transactions / args.users:.1f}")
        for table, count in counts.items():
            print(f"{table + ' created:':<22}{count}")
        print(f"Database:             {args.database}@{args.host}:{args.port}")
        print("=" * 60)
        print("\n✓ Data generation completed successfully!")

        db.close()

    except Exception as e:
        print(f"\n✗ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()