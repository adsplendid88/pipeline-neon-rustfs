import dlt
import duckdb
import os

def run_pipeline():
    print("🚀 Memulai Pipeline: GitHub -> DuckDB -> Neon")

    # ==========================================
    # TAHAP 0: BUAT FILE CSV CONTOH (jika belum ada)
    # ==========================================
    csv_path = "data.csv"
    
    if not os.path.exists(csv_path):
        print(" File CSV tidak ditemukan. Membuat file contoh...")
        # Buat file CSV dummy untuk testing
        csv_content = """id,name,department,salary,status
1,Budi Santoso,Engineering,8000000,ACTIVE
2,Siti Nurhaliza,Marketing,7500000,ACTIVE
3,Ahmad Wijaya,Engineering,9000000,ACTIVE
4,Dewi Lestari,HR,7000000,ACTIVE
5,Rudi Hartono,Finance,8500000,INACTIVE"""
        
        with open(csv_path, 'w') as f:
            f.write(csv_content)
        print(f"✅ File {csv_path} berhasil dibuat!")
    else:
        print(f"📂 Menggunakan file yang ada: {csv_path}")

    # ==========================================
    # TAHAP 1: EXTRACT (Baca CSV dari Repository)
    # ==========================================
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
        SELECT 
            id,
            UPPER(name) as name,
            department,
            salary,
            status
        FROM raw_data
        WHERE status = 'ACTIVE'
    """)
    
    clean_count = conn.execute("SELECT COUNT(*) FROM clean_data").fetchone()[0]
    print(f"✅ Data berhasil ditransformasi. Total data aktif: {clean_count}")

    # ==========================================
    # TAHAP 3: LOAD (DuckDB ke Neon)
    # ==========================================
    pipeline_load = dlt.pipeline(
        pipeline_name="github_to_neon",
        destination="postgres",
        dataset_name="hr_analytics"
    )
    
    final_data = conn.execute("SELECT * FROM clean_data").fetchall()
    
    load_info = pipeline_load.run(
        dlt.resource(final_data, name="employee_data", write_disposition="replace"),
        table_name="employee_data"
    )
    
    print("🎉 Data final berhasil masuk ke Neon!")
    print(f"📊 Tabel 'employee_data' berisi {clean_count} karyawan aktif")
    print(load_info)
    
    conn.close()

if __name__ == "__main__":
    run_pipeline() 
