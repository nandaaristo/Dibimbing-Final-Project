-- SQL script to create the main fact table for credit growth analysis
CREATE TABLE IF NOT EXISTS fact_credit_growth (
    id SERIAL PRIMARY KEY,
    kategori TEXT NOT NULL,              -- e.g. Kelompok Bank
    sektor TEXT NOT NULL,                -- Economic sector name
    bulan DATE NOT NULL,                -- Reporting month (YYYY-MM-DD)
    nilai_kredit NUMERIC,              -- Raw credit value (in billions IDR)
    mom_growth NUMERIC,                -- Month-over-Month growth (as decimal)
    flag_risiko BOOLEAN DEFAULT FALSE  -- True if MoM > 30% or other rule
);
