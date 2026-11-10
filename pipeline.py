import dlt
import duckdb
import os
import requests
import time

HF_API_URL = "https://api-inference.huggingface.co/models/cardiffnlp/twitter-roberta-base-sentiment-latest"

def get_hf_token():
    token = os.environ.get('HUGGINGFACE_API_TOKEN')
    if not token:
        raise ValueError("❌ HUGGINGFACE_API_TOKEN tidak ditemukan!")
    return token

def analyze_sentiment(text, hf_token):
    headers = {"Authorization": f"Bearer {hf_token}"}
    try:
        response = requests.post(HF_API_URL, headers=headers, json={"inputs": text[:512]})
        if response.status_code == 503:
            print("      ⏳ Model loading, tunggu 20 detik...")
            time.sleep(20)
            response = requests.post(HF_API_URL, headers=headers, json={"inputs": text[:512]})
        result = response.json()
        if isinstance(result, list) and len(result) > 0:
            scores = result[0]
            best = max(scores, key=lambda x: x['score'])
            return best['label'], round(best['score'], 4)
        return "UNKNOWN", 0.0
    except Exception as e:
        print(f"      ⚠️ Error: {e}")
        return "ERROR", 0.0

def run_pipeline():
    print("=" * 60)
    print("🚀 Memulai Pipeline: GitHub CSV -> DuckDB -> HuggingFace -> Neon")
    print("=" * 60)

    # TAHAP 0: Buat CSV contoh
    csv_path = "data.csv"
    if not os.path.exists(csv_path):
        print("\n📝 Membuat file CSV contoh...")
        csv_content = """id,employee_name,feedback_text,department
1,Budi Santoso,"Saya sangat senang bekerja di tim ini, lingkungan sangat suportif",Engineering
2,Siti Nurhaliza,"Deadline terlalu ketat, saya merasa burnout dan stress",Marketing
3,Ahmad Wijaya,"Proyeknya menantang dan saya belajar banyak hal baru",Engineering
4,Dewi Lestari,"Manajemen kurang memperhatikan work-life balance karyawan",HR
5,Rudi Hartono,"Fasilitas kantor bagus dan rekan kerja sangat ramah",Finance"""
        with open(csv_path, 'w', encoding='utf-8') as f:
            f.write(csv_content)
        print(f"✅ File {csv_path} dibuat!")
    else:
        print(f"\n📂 Menggunakan file: {csv_path}")

    # TAHAP 1: Extract CSV ke DuckDB
    print("\n📊 TAHAP 1: Extract data...")
    conn = duckdb.connect('pipeline.duckdb')
    conn.execute(f"CREATE OR REPLACE TABLE raw_data AS SELECT * FROM read_csv_auto('{csv_path}')")
    row_count = conn.execute("SELECT COUNT(*) FROM raw_data").fetchone()[0]
    print(f"✅ {row_count} baris data dibaca.")

    # TAHAP 2: AI Sentiment Analysis
    print("\n🤖 TAHAP 2: Analisis sentimen...")
    hf_token = get_hf_token()
    rows = conn.execute("SELECT id, employee_name, feedback_text, department FROM raw_data").fetchall()
    
    # PENTING: Gunakan LIST OF DICTIONARIES!
    enriched_data = []
    for idx, (emp_id, emp_name, feedback, dept) in enumerate(rows, 1):
        print(f"   [{idx}/{len(rows)}] {emp_name}...")
        sentiment, confidence = analyze_sentiment(feedback, hf_token)
        print(f"      → {sentiment} ({confidence})")
        
        enriched_data.append({
            'id': int(emp_id),
            'employee_name': emp_name,
            'feedback_text': feedback,
            'department': dept,
            'sentiment': sentiment,
            'confidence_score': confidence
        })
        time.sleep(1)
    
    print(f"\n✅ {len(enriched_data)} feedback dianalisis.")

    # TAHAP 3: Load ke Neon
    print("\n📤 TAHAP 3: Mengirim ke Neon...")
    pipeline_load = dlt.pipeline(
        pipeline_name="github_to_neon_with_ai",
        destination="postgres",
        dataset_name="hr_ai_analytics"
    )
    
    # Kirim LANGSUNG list of dicts (tanpa dlt.resource)
    load_info = pipeline_load.run(
        enriched_data,
        table_name="employee_sentiment",
        write_disposition="replace"
    )
    
    print("\n" + "=" * 60)
    print("🎉 PIPELINE BERHASIL!")
    print("=" * 60)
    print(load_info)
    conn.close()

if __name__ == "__main__":
    run_pipeline() 
