CREATE TABLE IF NOT EXISTS dm_fact_credit_growth_long (
    no SERIAL PRIMARY KEY,
    kode_sektor INT,
    sector VARCHAR,
    bank_type VARCHAR,
    kode_bank INT,
    date DATE,
    month VARCHAR,
    credit_growth NUMERIC,
    mom NUMERIC,
    is_abnormal BOOLEAN
);
