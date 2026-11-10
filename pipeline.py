import dlt
import duckdb
import os

def run_pipeline():
    print("🚀 Memulai Pipeline: GitHub -> DuckDB -> Neon")

    # ==========================================
    # TAHAP 1: EXTRACT (Baca CSV dari Repository GitHub)
    # ==========================================
    # File CSV ada di folder yang sama dengan pipeline.py
    csv_path = "data.csv"  # Ganti dengan nama file CSV kamu
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"❌ File {csv_path} tidak ditemukan di repository!")
    
    print(f"📂 Membaca file: {csv_path}")
    
    # Load CSV ke DuckDB langsung
    conn = duckdb.connect('pipeline.duckdb')
    conn.execute(f"""
        CREATE OR REPLACE TABLE raw_data AS 
        SELECT * FROM read_csv_auto('{csv_path}')
    """)
    
    row_count = conn.execute("SELECT COUNT(*) FROM raw_data").fetchone()[0]
    print(f"✅ Berhasil membaca {row_count} baris data dari CSV.")

    # ==========================================
    # TAHAP 2: TRANSFORM (DuckDB SQL)
    # ==========================================
    print("🧹 Membersihkan data dengan DuckDB...")
    conn.execute("""
        CREATE OR REPLACE TABLE clean_data AS 
        SELECT * FROM raw_data
    """)
    
    print("✅ Data berhasil ditransformasi di DuckDB.")

    # ==========================================
    # TAHAP 3: LOAD (DuckDB ke Neon)
    # ==========================================
    pipeline_load = dlt.pipeline(
        pipeline_name="github_to_neon",
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
