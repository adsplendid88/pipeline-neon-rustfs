import dlt
from dlt.sources.filesystem import filesystem
import duckdb

def run_pipeline():
    print("🚀 Memulai Pipeline: RustFS -> DuckDB -> Neon")

    # ==========================================
    # TAHAP 1: EXTRACT (RustFS ke DuckDB)
    # ==========================================
    # dlt menarik data dari RustFS dan menyimpannya ke DuckDB lokal
    pipeline_extract = dlt.pipeline(
        pipeline_name="rustfs_to_duckdb",
        destination="duckdb",
        dataset_name="raw_data"
    )
    
    # Ganti bucket_url dengan path folder data mentah kamu di RustFS
    # Contoh: "s3://my-bucket/hr_data/" atau "s3://my-bucket/sales/"
    data = filesystem(
       bucket_url="s3://test-bucket/", 
        file_glob="*.csv"  # Sesuaikan: *.csv, *.json, *.parquet
    )
    
    pipeline_extract.run(data)
    print("✅ Data mentah berhasil ditarik dari RustFS ke DuckDB.")

    # ==========================================
    # TAHAP 2: TRANSFORM (DuckDB SQL)
    # ==========================================
    # Koneksi ke file DuckDB yang baru saja dibuat
    conn = duckdb.connect('rustfs_to_duckdb.duckdb')
    
    # Contoh Transformasi 1: Bersihkan data (hapus baris kosong/null)
    print("🧹 Membersihkan data dengan DuckDB...")
    conn.execute("""
        CREATE OR REPLACE TABLE clean_data AS 
        SELECT * 
        FROM raw_data.data 
        WHERE id IS NOT NULL 
          AND id != ''
    """)
    
    # Contoh Transformasi 2: Agregasi (jika perlu)
    # conn.execute("""
    #     CREATE OR REPLACE TABLE aggregated_data AS
    #     SELECT 
    #         department,
    #         COUNT(*) as total_records,
    #         AVG(salary) as avg_salary
    #     FROM clean_data
    #     GROUP BY department
    # """)
    
    print("✅ Data berhasil ditransformasi di DuckDB.")

    # ==========================================
    # TAHAP 3: LOAD (DuckDB ke Neon)
    # ==========================================
    # dlt membaca tabel hasil transformasi dari DuckDB dan mengirim ke Neon
    pipeline_load = dlt.pipeline(
        pipeline_name="duckdb_to_neon",
        destination="postgres",
        dataset_name="analytics_data"
    )
    
    # Baca data dari DuckDB
    final_data = conn.execute("SELECT * FROM clean_data").fetchall()
    
    # Load ke Neon dengan write_disposition="replace" (ganti data lama)
    # Atau "append" jika ingin menambah data baru saja
    load_info = pipeline_load.run(
        dlt.resource(final_data, name="clean_dataset", write_disposition="replace"),
        table_name="clean_dataset"
    )
    
    print("🎉 Data final berhasil masuk ke Neon!")
    print(load_info)
    
    # Tutup koneksi DuckDB
    conn.close()

if __name__ == "__main__":
    run_pipeline()
