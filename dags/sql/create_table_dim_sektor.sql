CREATE TABLE IF NOT EXISTS dim_sektor (
    kode_sektor INT PRIMARY KEY,
    sector_name VARCHAR
);

INSERT INTO dim_sektor (kode_sektor, sector_name) VALUES
(01, 'Pinjaman Berdasarkan Lapangan Usaha'),
(02, 'Pertanian, Kehutanan & Perikanan'),
(03, 'Pertambangan dan Penggalian'),
(04, 'Industri Pengolahan'),
(05, 'Pengadaan Listrik dan Gas'),
(06, 'Pengadaan Air, Pengelolaan Sampah, Limbah dan Daur Ulang'),
(07, 'Konstruksi'),
(08, 'Perdagangan Besar dan Eceran, Reparasi Mobil dan Motor'),
(09, 'Transportasi dan Pergudangan'),
(10, 'Penyediaan Akomodasi dan Minum'),
(11, 'Informasi dan Komunikasi'),
(12, 'Jasa Keuangan dan Asuransi'),
(13, 'Real Estate'),
(14, 'Jasa Perusahaan'),
(15, 'Administrasi Pemerintahan, Pertahanan dan Jaminan Sosial Wajib'),
(16, 'Jasa Pendidikan'),
(17, 'Jasa Kesehatan dan Kegiatan Lainnya'),
(18, 'Jasa Lainnya'),
(19, 'Pinjaman Kepada Bukan Lapangan Usaha (Konsumsi Rumah Tangga)'),
(20, 'Rumah Tinggal'),
(21, 'Flat dan Apartemen'),
(22, 'Rumah Toko (Ruko) dan Rumah Kantor (Rukan)'),
(23, 'Kendaraan Bermotor'),
(24, 'Lainnya');
