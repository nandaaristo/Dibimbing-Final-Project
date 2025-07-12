CREATE TABLE IF NOT EXISTS dm_fact_credit_growth_clean (
    no INTEGER,
    date DATE,
    kode_sektor INT,
    sector TEXT,
    bank_type TEXT,
    kode_bank INT,
    jan NUMERIC,
    feb NUMERIC,
    mar NUMERIC,
    apr NUMERIC,
    may NUMERIC,
    mom_feb NUMERIC,
    mom_mar NUMERIC,
    mom_apr NUMERIC,
    mom_may NUMERIC,
    is_abnormal TEXT
);

