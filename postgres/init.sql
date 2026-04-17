DROP TABLE IF EXISTS case_decisions;
DROP TABLE IF EXISTS case_priority_history;
DROP TABLE IF EXISTS aml_enriched_cases;
DROP TABLE IF EXISTS aml_cases;
DROP TABLE IF EXISTS customers;

CREATE TABLE aml_cases (
    alert_id UUID PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    transaction_id VARCHAR(50),
    pattern_type VARCHAR(50) NOT NULL,
    alert_message TEXT NOT NULL,
    risk_score INT NOT NULL,
    priority_score INT DEFAULT 0,
    total_amount NUMERIC(18,2),
    transaction_time TIMESTAMP,
    evidence JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'OPEN',
    sla_deadline TIMESTAMP DEFAULT CURRENT_TIMESTAMP + INTERVAL '1 day'
);

CREATE TABLE customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    kyc_status VARCHAR(50) DEFAULT 'STANDARD',
    account_open_date DATE DEFAULT CURRENT_DATE,
    risk_segment VARCHAR(50) DEFAULT 'MEDIUM'
);

CREATE TABLE aml_enriched_cases (
    alert_id UUID PRIMARY KEY REFERENCES aml_cases(alert_id),
    customer_id VARCHAR(50) NOT NULL,
    pattern_type VARCHAR(50) NOT NULL,
    risk_score INT NOT NULL,
    priority_score INT DEFAULT 0,
    kyc_status VARCHAR(50),
    account_age_days INT,
    risk_segment VARCHAR(50),
    evidence JSONB,
    enriched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE case_decisions (
    decision_id SERIAL PRIMARY KEY,
    alert_id UUID REFERENCES aml_cases(alert_id),
    analyst_id VARCHAR(100) NOT NULL,
    outcome VARCHAR(50) NOT NULL,
    notes TEXT,
    decided_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE case_priority_history (
    history_id SERIAL PRIMARY KEY,
    alert_id UUID REFERENCES aml_cases(alert_id),
    old_priority_score INT,
    new_priority_score INT,
    old_status VARCHAR(20),
    new_status VARCHAR(20),
    reason TEXT,
    rescored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
