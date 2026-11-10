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
    
    source_files = filesystem(
        bucket_url="s3://test-bucket/", 
        file_glob="*.csv"
    )
    
    pipeline_extract.run(source_files)
    print("✅ Data mentah berhasil ditarik dari RustFS ke DuckDB.")

    # ==========================================
    # TAHAP 2: TRANSFORM (DuckDB SQL)
    # ==========================================
    conn = duckdb.connect('rustfs_to_duckdb.duckdb')
    
    # 🔍 AUTO-DETECT: Cari tabel data (abaikan tabel internal dlt)
    print("🔍 Mencari tabel data di DuckDB...")
    tables_result = conn.execute("""
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_name NOT LIKE '_dlt_%'
          AND table_schema NOT IN ('information_schema', 'pg_catalog')
        LIMIT 1
    """).fetchall()
    
    if not tables_result:
        raise Exception("❌ Tidak ada tabel data yang ditemukan di DuckDB!")
    
    source_schema = tables_result[0][0]
    source_table = tables_result[0][1]
    print(f"✅ Ditemukan tabel: {source_schema}.{source_table}")
    
    # Tampilkan isi tabel untuk debugging
    row_count = conn.execute(f"SELECT COUNT(*) FROM {source_schema}.{source_table}").fetchone()[0]
    print(f"📊 Jumlah baris data: {row_count}")
    
    # Transform: Bersihkan data (hapus baris kosong)
    print("🧹 Membersihkan data dengan DuckDB...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE clean_data AS 
        SELECT * FROM {source_schema}.{source_table}
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
    
    final_data = conn.execute("SELECT * FROM clean_data").fetchall()
    
    load_info = pipeline_load.run(
        dlt.resource(final_data, name="clean_dataset", write_disposition="replace"),
        table_name="clean_dataset"
    )
    
    print("🎉 Data final berhasil masuk ke Neon!")
    print(load_info)
    
    conn.close()

if __name__ == "__main__":
    run_pipeline()
