# Contoh Data Katalog Gold Layer Untuk Client1, setiap client akan dibuat data katalog untuk Tim lain.

## Ikhtisar

Lapisan Gold adalah representasi data pada level bisnis, disusun untuk mendukung kebutuhan analitik dan pelaporan. Lapisan ini terdiri dari **tabel dimensi** dan **tabel fakta** yang menyimpan metrik bisnis spesifik.

---

### 1. **gold_client1.dim_customers**

- **Tujuan:** Menyimpan detail pelanggan yang diperkaya dengan data demografis dan geografis.
- **Kolom:**

| Column Name     | Data Type    | Description                                                                               |
| --------------- | ------------ | ----------------------------------------------------------------------------------------- |
| customer_key    | INT          | Surrogate key yang mengidentifikasi secara unik setiap record pelanggan di tabel dimensi. |
| customer_id     | INT          | Identifier numerik unik yang diberikan untuk setiap pelanggan.                            |
| customer_number | NVARCHAR(50) | Identifier alfanumerik yang mewakili pelanggan, digunakan untuk pelacakan dan referensi.  |
| first_name      | NVARCHAR(50) | Nama depan pelanggan sebagaimana tercatat di sistem.                                      |
| last_name       | NVARCHAR(50) | Nama belakang atau nama keluarga pelanggan.                                               |
| country         | NVARCHAR(50) | Negara tempat tinggal pelanggan (mis. 'Australia').                                       |
| marital_status  | NVARCHAR(50) | Status pernikahan pelanggan (mis. 'Married', 'Single').                                   |
| gender          | NVARCHAR(50) | Jenis kelamin pelanggan (mis. 'Male', 'Female', 'n/a').                                   |
| birthdate       | DATE         | Tanggal lahir pelanggan, diformat YYYY-MM-DD (mis. 1971-10-06).                           |
| create_date     | DATE         | Tanggal dan waktu ketika record pelanggan dibuat dalam sistem.                            |

---

### 2. **gold_client1.dim_products**

- **Tujuan:** Menyediakan informasi tentang produk dan atribut-atributnya.
- **Kolom:**

| Column Name          | Data Type    | Description                                                                                   |
| -------------------- | ------------ | --------------------------------------------------------------------------------------------- |
| product_key          | INT          | Surrogate key yang mengidentifikasi secara unik setiap record produk di tabel dimensi produk. |
| product_id           | INT          | Identifier unik yang diberikan pada produk untuk pelacakan internal dan referensi.            |
| product_number       | NVARCHAR(50) | Kode alfanumerik terstruktur yang merepresentasikan produk, sering dipakai untuk inventori.   |
| product_name         | NVARCHAR(50) | Nama deskriptif produk, mencakup detail kunci seperti tipe, warna, dan ukuran.                |
| category_id          | NVARCHAR(50) | Identifier unik untuk kategori produk, menghubungkan ke klasifikasi tingkat atas.             |
| category             | NVARCHAR(50) | Klasifikasi luas produk (mis. Bikes, Components) untuk mengelompokkan item terkait.           |
| subcategory          | NVARCHAR(50) | Klasifikasi yang lebih rinci di dalam kategori, mis. tipe produk.                             |
| maintenance_required | NVARCHAR(50) | Menunjukkan apakah produk memerlukan pemeliharaan (mis. 'Yes', 'No').                         |
| cost                 | INT          | Biaya atau harga dasar produk, dalam satuan mata uang (angka bulat).                          |
| product_line         | NVARCHAR(50) | Lini atau seri produk spesifik tempat produk termasuk (mis. Road, Mountain).                  |
| start_date           | DATE         | Tanggal ketika produk tersedia untuk dijual atau digunakan.                                   |

---

### 3. **gold_client1.fact_sales**

- **Tujuan:** Menyimpan data transaksi penjualan untuk tujuan analitik.
- **Kolom:**

| Column Name   | Data Type    | Description                                                                            |
| ------------- | ------------ | -------------------------------------------------------------------------------------- |
| order_number  | NVARCHAR(50) | Identifier alfanumerik unik untuk setiap order penjualan (mis. 'SO54496').             |
| product_key   | INT          | Surrogate key yang menghubungkan order ke tabel dimensi produk.                        |
| customer_key  | INT          | Surrogate key yang menghubungkan order ke tabel dimensi pelanggan.                     |
| order_date    | DATE         | Tanggal ketika order dibuat.                                                           |
| shipping_date | DATE         | Tanggal ketika order dikirim ke pelanggan.                                             |
| due_date      | DATE         | Tanggal jatuh tempo pembayaran untuk order.                                            |
| sales_amount  | INT          | Nilai total penjualan untuk baris item tersebut, dalam satuan mata uang (angka bulat). |
| quantity      | INT          | Jumlah unit produk yang dipesan untuk baris item tersebut (mis. 1).                    |
| price         | INT          | Harga per unit produk untuk baris item tersebut, dalam satuan mata uang (angka bulat). |
