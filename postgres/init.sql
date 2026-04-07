DROP TABLE IF EXISTS analyst_actions;
DROP TABLE IF EXISTS aml_cases;

CREATE TABLE aml_cases (
    case_id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    transaction_id VARCHAR(50),
    pattern_type VARCHAR(50) NOT NULL,
    alert_message TEXT NOT NULL,
    risk_score INT NOT NULL,
    amount NUMERIC(18,2),
    transaction_time TIMESTAMP,
    evidence JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'OPEN'
);

CREATE TABLE analyst_actions (
    action_id SERIAL PRIMARY KEY,
    case_id INT REFERENCES aml_cases(case_id),
    analyst_name VARCHAR(100),
    action_taken VARCHAR(100),
    notes TEXT,
    action_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);