-- models/marts/fact_credit_growth_clean.sql
SELECT
    no,
    sector,
    kode_sektor,
    bank_type,
    kode_bank,
    jan,
    feb,
    mar,
    apr,
    may,
    ROUND((may - apr)/apr, 4) AS mom_growth,
    CASE WHEN ((may - apr)/apr) > 0.3 THEN 'TRUE' ELSE 'FALSE' END AS is_abnormal,
    tanggal
FROM {{ ref('stg_fact_credit_growth') }}
