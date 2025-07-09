CREATE TABLE IF NOT EXISTS dim_bank_type (
    bank_type INT PRIMARY KEY,
    bank_category VARCHAR
);

INSERT INTO dim_bank_type (bank_type, bank_category) VALUES
(01, 'Bank Persero'),
(02, 'Bank Pemerintah Daerah'),
(03, 'Bank Swasta Nasional'),
(04, 'Kantor Cabang di Luar Negeri'),
(05, 'Bank Perkreditan Rakyat'),
(06, 'Jumlah Semua Bank');
