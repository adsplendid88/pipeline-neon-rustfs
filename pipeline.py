import dlt
from dlt.sources.filesystem import filesystem
import duckdb

def run_pipeline():
    print("🚀 Memulai Pipeline: RustFS -> DuckDB -> Neon")

    # ==========================================
    # TAHAP 1: EXTRACT (RustFS ke DuckDB)
    # ==========================================
    pipeline_extract = dlt.pipeline(
        pipeline_name="rustfs_to_duckdb",
        destination="duckdb",
        dataset_name="raw_data"
    )
    
    # PENTING: Beri nama resource secara eksplisit agar nama tabel di DuckDB jelas
    # Kita beri nama "csv_files", jadi tabelnya nanti bernama 'csv_files'
    source_files = filesystem(
        bucket_url="s3://test-bucket/", 
        file_glob="*.csv"
    ).with_name("csv_files")
    
    pipeline_extract.run(source_files)
    print("✅ Data mentah berhasil ditarik dari RustFS ke DuckDB.")
    
    # Hapus variabel python ini agar tidak bentrok dengan DuckDB saat query SQL
    del source_files

    # ==========================================
    # TAHAP 2: TRANSFORM (DuckDB SQL)
    # ==========================================
    # Koneksi ke file DuckDB lokal yang dibuat oleh dlt
    conn = duckdb.connect('rustfs_to_duckdb.duckdb')
    
    print("🧹 Membersihkan data dengan DuckDB...")
    
    # Query SQL: Ambil semua data dari tabel 'csv_files' di schema 'raw_data'
    # (Sesuaikan WHERE clause jika perlu, contoh di bawah ini mengambil semua baris)
    conn.execute("""
        CREATE OR REPLACE TABLE clean_data AS 
        SELECT * 
        FROM raw_data.csv_files
    """)
    
    print("✅ Data berhasil ditransformasi di DuckDB.")

    # ==========================================
    # TAHAP 3: LOAD (DuckDB ke Neon)
    # ==========================================
    pipeline_load = dlt.pipeline(
        pipeline_name="duckdb_to_neon",
        destination="postgres",
        dataset_name="analytics_data"
    )
    
    # Baca data bersih dari DuckDB
    final_data = conn.execute("SELECT * FROM clean_data").fetchall()
    
    # Kirim ke Neon
    load_info = pipeline_load.run(
        dlt.resource(final_data, name="clean_dataset", write_disposition="replace"),
        table_name="clean_dataset"
    )
    
    print("🎉 Data final berhasil masuk ke Neon!")
    print(load_info)
    
    # Tutup koneksi
    conn.close()

if __name__ == "__main__":
    run_pipeline()
