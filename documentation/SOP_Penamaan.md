SOP Penamaan

Dokumen ini menjelaskan konvensi penamaan yang digunakan untuk schema, tabel, view, kolom, dan objek lainnya dalam data warehouse.

Daftar Isi
1. Prinsip Umum
2. Konvensi Penamaan Tabel
   - Aturan Bronze
   - Aturan Silver
   - Aturan Gold
3. Konvensi Penamaan Kolom
   - Surrogate Key
   - Kolom Teknis
4. Prosedur Tersimpan (Stored Procedure)

Prinsip Umum
- Konvensi Penamaan: Gunakan format snake_case, yaitu huruf kecil semua dan kata dipisahkan dengan garis bawah (_).
- Bahasa: Gunakan bahasa Inggris untuk semua nama objek.
- Hindari Kata yang Dipesan (Reserved Words): Jangan gunakan kata-kata SQL yang telah dipesan sebagai nama objek.

Konvensi Penamaan Tabel

Aturan Bronze
- Nama tabel harus diawali dengan nama sistem sumber.
- Nama tabel harus sesuai dengan nama asli dari sistem sumber, tanpa perubahan.
- Pola: <sourcesystem>_<entity>
  - <sourcesystem>: Nama sistem sumber, contoh: crm, erp.
  - <entity>: Nama asli tabel dari sistem sumber.
  - Contoh: crm_customer_info → Data customer dari sistem CRM.

Aturan Silver
- Sama seperti Bronze, nama tabel diawali dengan nama sistem sumber.
- Tidak boleh mengubah nama asli tabel dari sumber.
- Pola: <sourcesystem>_<entity>
  - <sourcesystem>: Nama sistem sumber, contoh: crm, erp.
  - <entity>: Nama asli tabel dari sistem sumber.
  - Contoh: crm_customer_info → Data customer dari sistem CRM.

Aturan Gold
- Nama tabel harus mencerminkan makna bisnis dan diawali dengan kategori.
- Pola: <category>_<entity>
  - <category>: Jenis tabel, contoh: dim (dimensi), fact (fakta).
  - <entity>: Nama deskriptif yang sesuai dengan domain bisnis.
  - Contoh:
    - dim_customers → Tabel dimensi untuk data pelanggan.
    - fact_sales → Tabel fakta untuk transaksi penjualan.

Glosarium Pola Kategori
| Pola       | Arti                          | Contoh                                     |
|------------|-------------------------------|--------------------------------------------|
| dim_       | Tabel dimensi                 | dim_customer, dim_product                  |
| fact_      | Tabel fakta                   | fact_sales                                 |
| report_    | Tabel laporan                 | report_customers, report_sales_monthly     |

Konvensi Penamaan Kolom

Surrogate Key
- Semua primary key pada tabel dimensi harus menggunakan akhiran _key.
- Pola: <table_name>_key
  - <table_name>: Nama tabel atau entitasnya.
  - _key: Menandakan bahwa kolom ini adalah surrogate key.
  - Contoh: customer_key → Primary key di tabel dim_customers.

Kolom Teknis
- Kolom teknis harus memiliki awalan dwh_ dan diikuti dengan nama yang menjelaskan fungsi kolom tersebut.
- Pola: dwh_<column_name>
  - dwh: Penanda bahwa ini adalah kolom teknis/metadata.
  - <column_name>: Nama deskriptif dari fungsi kolom.
  - Contoh: dwh_load_date → Tanggal ketika data dimuat ke dalam DWH.

Prosedur Tersimpan (Stored Procedure)
- Semua stored procedure yang digunakan untuk proses loading data harus mengikuti pola: load_<layer>
  - <layer>: Lapisan yang sedang dimuat, contoh: bronze, silver, gold.
  - Contoh:
    - load_bronze → Procedure untuk memuat data ke lapisan Bronze.
    - load_silver → Procedure untuk memuat data ke lapisan Silver.
