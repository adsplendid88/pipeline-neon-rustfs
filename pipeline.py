import dlt
import duckdb
import os
import requests
import time

# Konfigurasi Hugging Face Inference API
# Model: cardiffnlp/twitter-roberta-base-sentiment-latest (ringan & akurat)
HF_API_URL = "https://api-inference.huggingface.co/models/cardiffnlp/twitter-roberta-base-sentiment-latest"

def get_hf_token():
    """Ambil token dari environment variable GitHub Secrets"""
    token = os.environ.get('HUGGINGFACE_API_TOKEN')
    if not token:
        raise ValueError("❌ HUGGINGFACE_API_TOKEN tidak ditemukan di environment!")
    return token

def analyze_sentiment(text, hf_token):
    """Kirim teks ke Hugging Face API untuk analisis sentimen"""
    headers = {"Authorization": f"Bearer {hf_token}"}
    
    try:
        response = requests.post(
            HF_API_URL,
            headers=headers,
            json={"inputs": text[:512]}  # Batasi 512 karakter
        )
        
        # Handle rate limiting
        if response.status_code == 503:
            print("      ⏳ Model sedang loading, menunggu 20 detik...")
            time.sleep(20)
            response = requests.post(HF_API_URL, headers=headers, json={"inputs": text[:512]})
        
        result = response.json()
        
        # Parse hasil dari API
        if isinstance(result, list) and len(result) > 0:
            scores = result[0]
            # Cari label dengan score tertinggi
            best_result = max(scores, key=lambda x: x['score'])
            return best_result['label'], round(best_result['score'], 4)
        else:
            print(f"      ⚠️ Response tidak terduga: {result}")
            return "UNKNOWN", 0.0
    except Exception as e:
        print(f"      ⚠️ Error analisis sentimen: {e}")
        return "ERROR", 0.0

def run_pipeline():
    print("=" * 60)
    print("🚀 Memulai Pipeline: GitHub CSV -> DuckDB -> HuggingFace -> Neon")
    print("=" * 60)

    # ==========================================
    # TAHAP 0: BUAT FILE CSV CONTOH (jika belum ada)
    # ==========================================
    csv_path = "data.csv"
    
    if not os.path.exists(csv_path):
        print("\n📝 File CSV tidak ditemukan. Membuat file contoh dengan data feedback karyawan...")
        csv_content = """id,employee_name,feedback_text,department
1,Budi Santoso,"Saya sangat senang bekerja di tim ini, lingkungan sangat suportif",Engineering
2,Siti Nurhaliza,"Deadline terlalu ketat, saya merasa burnout dan stress",Marketing
3,Ahmad Wijaya,"Proyeknya menantang dan saya belajar banyak hal baru",Engineering
4,Dewi Lestari,"Manajemen kurang memperhatikan work-life balance karyawan",HR
5,Rudi Hartono,"Fasilitas kantor bagus dan rekan kerja sangat ramah",Finance"""
        
        with open(csv_path, 'w', encoding='utf-8') as f:
            f.write(csv_content)
        print(f"✅ File {csv_path} berhasil dibuat!")
    else:
        print(f"\n📂 Menggunakan file yang ada: {csv_path}")

    # ==========================================
    # TAHAP 1: EXTRACT (Baca CSV ke DuckDB)
    # ==========================================
    print("\n📊 TAHAP 1: Extract data dari CSV...")
    conn = duckdb.connect('pipeline.duckdb')
    conn.execute(f"""
        CREATE OR REPLACE TABLE raw_data AS 
        SELECT * FROM read_csv_auto('{csv_path}')
    """)
    
    row_count = conn.execute("SELECT COUNT(*) FROM raw_data").fetchone()[0]
    print(f"✅ Berhasil membaca {row_count} baris data dari CSV.")

    # ==========================================
    # TAHAP 2: TRANSFORM + AI (DuckDB + Hugging Face)
    # ==========================================
    print("\n🤖 TAHAP 2: Menganalisis sentimen dengan Hugging Face AI...")
    
    # Ambil token Hugging Face
    hf_token = get_hf_token()
    print(f"✅ Token Hugging Face berhasil dimuat.")
    
    # Ambil data feedback dari DuckDB
    rows = conn.execute("SELECT id, employee_name, feedback_text, department FROM raw_data").fetchall()
    
    enriched_data = []
    for idx, (emp_id, emp_name, feedback, dept) in enumerate(rows, 1):
        print(f"   [{idx}/{len(rows)}] Menganalisis feedback dari {emp_name}...")
        
        # Kirim ke Hugging Face API
        sentiment, confidence = analyze_sentiment(feedback, hf_token)
        print(f"      → Sentiment: {sentiment} (confidence: {confidence})")
        
        enriched_data.append((int(emp_id), emp_name, feedback, dept, sentiment, confidence))
        
        # Tambahkan delay kecil untuk menghindari rate limit
        time.sleep(1)
    
    # Simpan hasil AI ke tabel baru di DuckDB
    conn.execute("""
        CREATE OR REPLACE TABLE ai_enriched_data (
            id INTEGER,
            employee_name VARCHAR,
            feedback_text VARCHAR,
            department VARCHAR,
            sentiment VARCHAR,
            confidence_score DOUBLE
        )
    """)
    
    conn.executemany("""
        INSERT INTO ai_enriched_data VALUES (?, ?, ?, ?, ?, ?)
    """, enriched_data)
    
    print(f"\n✅ AI berhasil menganalisis {len(enriched_data)} feedback karyawan.")

    # ==========================================
    # TAHAP 3: LOAD (DuckDB ke Neon)
    # ==========================================
    print("\n📤 TAHAP 3: Mengirim data ke Neon...")
    pipeline_load = dlt.pipeline(
        pipeline_name="github_to_neon_with_ai",
        destination="postgres",
        dataset_name="hr_ai_analytics"
    )
    
    final_data = conn.execute("SELECT * FROM ai_enriched_data").fetchall()
    
    load_info = pipeline_load.run(
        dlt.resource(final_data, name="employee_sentiment", write_disposition="replace"),
        table_name="employee_sentiment"
    )
    
    print("\n" + "=" * 60)
    print("🎉 PIPELINE BERHASIL!")
    print("=" * 60)
    print(f"📊 Tabel 'employee_sentiment' berisi {len(enriched_data)} data feedback")
    print(f"🗄️  Database: Neon (Postgres)")
    print(f"📂 Dataset: hr_ai_analytics")
    print(load_info)
    
    conn.close()

if __name__ == "__main__":
    run_pipeline()
