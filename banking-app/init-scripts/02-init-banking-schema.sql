-- Banking application schema initialization
--
-- Six tables: users and their accounts, the transactions against them, the
-- merchants on the other side, the lending book, and an audit trail.
--
-- The later four deliberately use Postgres types the first two do not - uuid,
-- jsonb, text[], interval, time, inet, bytea. The Postgres-to-Iceberg blueprint
-- maps a fixed list of types and silently falls back to StringType for anything
-- else, so real columns of those kinds make that behaviour visible in the DAG
-- generator's warnings instead of theoretical.

-- Drop in dependency order: children before the parents they reference.
DROP TABLE IF EXISTS audit_log CASCADE;
DROP TABLE IF EXISTS loans CASCADE;
DROP TABLE IF EXISTS merchants CASCADE;
DROP TABLE IF EXISTS accounts CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- ---------------------------------------------------------------------------
-- users: customers, carrying a denormalised primary account for convenience.
-- ---------------------------------------------------------------------------
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    phone VARCHAR(20),
    date_of_birth DATE NOT NULL,
    ssn VARCHAR(11) UNIQUE,
    address VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    account_type VARCHAR(20) NOT NULL CHECK (account_type IN ('checking', 'savings', 'credit', 'investment')),
    account_number VARCHAR(20) UNIQUE NOT NULL,
    balance NUMERIC(15, 2) DEFAULT 0.00 CHECK (balance >= 0),
    credit_score INTEGER CHECK (credit_score BETWEEN 300 AND 850),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- transactions: movement of money. `merchant` is free text here; the merchants
-- table below is the reference form.
-- ---------------------------------------------------------------------------
CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(user_id) ON DELETE CASCADE,
    transaction_type VARCHAR(20) NOT NULL CHECK (transaction_type IN ('deposit', 'withdrawal', 'transfer', 'payment', 'fee', 'interest')),
    amount NUMERIC(15, 2) NOT NULL CHECK (amount > 0),
    balance_after NUMERIC(15, 2) NOT NULL CHECK (balance_after >= 0),
    transaction_date TIMESTAMP NOT NULL,
    description VARCHAR(200),
    category VARCHAR(50),
    merchant VARCHAR(100),
    status VARCHAR(20) DEFAULT 'completed' CHECK (status IN ('pending', 'completed', 'failed', 'cancelled')),
    reference_number VARCHAR(50) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- accounts: normalised form of users.account_number, so a user may hold several.
-- ---------------------------------------------------------------------------
CREATE TABLE accounts (
    account_id          BIGSERIAL PRIMARY KEY,
    user_id             INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    account_number      VARCHAR(20) UNIQUE NOT NULL,
    account_type        VARCHAR(20) NOT NULL
                        CHECK (account_type IN ('checking', 'savings', 'credit', 'investment')),
    balance             NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
    available_balance   NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
    interest_rate       REAL,
    overdraft_limit     DOUBLE PRECISION DEFAULT 0,
    currency            CHAR(3) NOT NULL DEFAULT 'USD',
    is_primary          BOOLEAN DEFAULT FALSE,
    opened_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    closed_at           TIMESTAMP,
    last_activity_at    TIMESTAMPTZ,
    status              VARCHAR(20) NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active', 'frozen', 'closed', 'dormant'))
);

-- ---------------------------------------------------------------------------
-- merchants: transaction counterparties.
-- Unmapped types: uuid, text[], jsonb.
-- ---------------------------------------------------------------------------
CREATE TABLE merchants (
    merchant_id         SERIAL PRIMARY KEY,
    merchant_uuid       UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    name                VARCHAR(100) NOT NULL,
    legal_name          TEXT,
    category            VARCHAR(50) NOT NULL,
    mcc_code            CHAR(4),
    country             CHAR(2) NOT NULL DEFAULT 'US',
    city                VARCHAR(100),
    website             TEXT,
    tags                TEXT[],
    metadata            JSONB,
    risk_score          SMALLINT CHECK (risk_score BETWEEN 0 AND 100),
    is_active           BOOLEAN DEFAULT TRUE,
    onboarded_on        DATE NOT NULL DEFAULT CURRENT_DATE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- loans: lending book. Unmapped types: interval, time.
-- ---------------------------------------------------------------------------
CREATE TABLE loans (
    loan_id             BIGSERIAL PRIMARY KEY,
    user_id             INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    account_id          BIGINT REFERENCES accounts(account_id) ON DELETE SET NULL,
    loan_type           VARCHAR(20) NOT NULL
                        CHECK (loan_type IN ('mortgage', 'auto', 'personal', 'student', 'credit_line')),
    principal_amount    NUMERIC(15, 2) NOT NULL CHECK (principal_amount > 0),
    outstanding_balance NUMERIC(15, 2) NOT NULL CHECK (outstanding_balance >= 0),
    interest_rate       NUMERIC(5, 3) NOT NULL,
    term_months         SMALLINT NOT NULL CHECK (term_months > 0),
    monthly_payment     NUMERIC(15, 2) NOT NULL,
    grace_period        INTERVAL,
    payment_due_time    TIME,
    origination_date    DATE NOT NULL,
    maturity_date       DATE NOT NULL,
    next_payment_date   DATE,
    status              VARCHAR(20) NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active', 'paid_off', 'delinquent', 'defaulted', 'refinanced')),
    collateral_desc     TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT loans_maturity_after_origination CHECK (maturity_date > origination_date)
);

-- ---------------------------------------------------------------------------
-- audit_log: append-only trail, so the natural table for testing `mode: append`
-- against the blueprint's default overwrite.
-- Unmapped types: uuid, inet, bytea, jsonb.
-- ---------------------------------------------------------------------------
CREATE TABLE audit_log (
    audit_id            BIGSERIAL PRIMARY KEY,
    event_uuid          UUID NOT NULL DEFAULT gen_random_uuid(),
    user_id             INTEGER REFERENCES users(user_id) ON DELETE SET NULL,
    entity_type         VARCHAR(50) NOT NULL,
    entity_id           BIGINT,
    action              VARCHAR(50) NOT NULL
                        CHECK (action IN ('create', 'read', 'update', 'delete', 'login', 'logout', 'failed_login')),
    changed_fields      JSONB,
    ip_address          INET,
    user_agent          TEXT,
    session_token       BYTEA,
    occurred_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    duration_ms         INTEGER,
    succeeded           BOOLEAN NOT NULL DEFAULT TRUE
);

-- Indexes
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_account_number ON users(account_number);
CREATE INDEX idx_users_account_type ON users(account_type);
CREATE INDEX idx_users_is_active ON users(is_active);
CREATE INDEX idx_users_created_at ON users(created_at);

CREATE INDEX idx_transactions_user_id ON transactions(user_id);
CREATE INDEX idx_transactions_date ON transactions(transaction_date);
CREATE INDEX idx_transactions_type ON transactions(transaction_type);
CREATE INDEX idx_transactions_status ON transactions(status);
CREATE INDEX idx_transactions_category ON transactions(category);
CREATE INDEX idx_transactions_created_at ON transactions(created_at);

CREATE INDEX idx_accounts_user_id ON accounts(user_id);
CREATE INDEX idx_accounts_status ON accounts(status);
CREATE INDEX idx_accounts_type ON accounts(account_type);

CREATE INDEX idx_merchants_category ON merchants(category);
CREATE INDEX idx_merchants_country ON merchants(country);
CREATE INDEX idx_merchants_metadata ON merchants USING GIN (metadata);

CREATE INDEX idx_loans_user_id ON loans(user_id);
CREATE INDEX idx_loans_status ON loans(status);
CREATE INDEX idx_loans_maturity ON loans(maturity_date);

CREATE INDEX idx_audit_user_id ON audit_log(user_id);
CREATE INDEX idx_audit_occurred_at ON audit_log(occurred_at);
CREATE INDEX idx_audit_entity ON audit_log(entity_type, entity_id);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO bankinguser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO bankinguser;

-- Add comments for documentation
COMMENT ON TABLE users IS 'Banking application users with account information';
COMMENT ON TABLE transactions IS 'User transaction history';
COMMENT ON TABLE accounts IS 'Bank accounts; normalised form of users.account_number';
COMMENT ON TABLE merchants IS 'Transaction counterparties';
COMMENT ON TABLE loans IS 'Lending book';
COMMENT ON TABLE audit_log IS 'Append-only audit trail of user and system actions';

COMMENT ON COLUMN users.ssn IS 'Social Security Number in format XXX-XX-XXXX';
COMMENT ON COLUMN users.account_type IS 'Type of account: checking, savings, credit, or investment';
COMMENT ON COLUMN users.credit_score IS 'Credit score between 300 and 850';

COMMENT ON COLUMN transactions.transaction_type IS 'Type: deposit, withdrawal, transfer, payment, fee, or interest';
COMMENT ON COLUMN transactions.status IS 'Status: pending, completed, failed, or cancelled';
COMMENT ON COLUMN transactions.reference_number IS 'Unique transaction reference number';

COMMENT ON COLUMN merchants.mcc_code IS 'ISO 18245 merchant category code';
COMMENT ON COLUMN merchants.metadata IS 'Unstructured attributes; JSONB is unmapped by the Iceberg blueprint and lands as a string';
COMMENT ON COLUMN loans.grace_period IS 'INTERVAL is unmapped by the Iceberg blueprint and lands as a string';
COMMENT ON COLUMN audit_log.ip_address IS 'INET is unmapped by the Iceberg blueprint and lands as a string';
COMMENT ON COLUMN audit_log.session_token IS 'BYTEA is unmapped by the Iceberg blueprint and lands as a string';
