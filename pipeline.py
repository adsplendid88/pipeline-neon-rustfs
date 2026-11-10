import dlt
import duckdb
import os
import requests
import time

# Hugging Face API
HF_API_URL = "https://api-inference.huggingface.co/models/distilbert-base-uncased-finetuned-sst-2-english"

# Fallback: Rule-based sentiment analysis (bahasa Indonesia & Inggris)
POSITIVE_WORDS = [
    'senang', 'bahagia', 'suka', 'bagus', 'hebat', 'mantap', 'sukses',
    'happy', 'good', 'great', 'excellent', 'wonderful', 'amazing', 'love',
    'supportive', 'friendly', 'challenging', 'learn', 'great'
]

NEGATIVE_WORDS = [
    'sedih', 'marah', 'kecewa', 'buruk', 'jelek', 'gagal', 'stress',
    'burnout', 'toxic', 'sulit', 'masalah', 'komplain',
    'sad', 'angry', 'bad', 'terrible', 'awful', 'hate', 'stressed',
    'burned out', 'tight deadline', 'does not care'
]

def analyze_sentiment_simple(text):
    """Rule-based sentiment analysis sebagai fallback"""
    text_lower = text.lower()
    
    positive_count = sum(1 for word in POSITIVE_WORDS if word in text_lower)
    negative_count = sum(1 for word in NEGATIVE_WORDS if word in text_lower)
    
    if positive_count > negative_count:
        return "LABEL_1", 0.85  # Positive
    elif negative_count > positive_count:
        return "LABEL_0", 0.85  # Negative
    else:
        return "LABEL_1", 0.50  # Neutral (default positive)

def get_hf_token():
    token = os.environ.get('HUGGINGFACE_API_TOKEN')
    if not token:
        print("⚠️ Token tidak ditemukan, akan pakai fallback")
        return None
    print(f"✅ Token ditemukan (panjang: {len(token)} karakter)")
    return token

def analyze_sentiment_hf(text, hf_token):
    """Coba Hugging Face API, jika gagal pakai fallback"""
    if not hf_token:
        print("      🔄 Pakai fallback (rule-based)")
        return analyze_sentiment_simple(text)
    
    headers = {"Authorization": f"Bearer {hf_token}"}
    
    try:
        print(f"      📤 Mengirim ke Hugging Face API...")
        response = requests.post(
            HF_API_URL, 
            headers=headers, 
            json={"inputs": text[:512]},
            timeout=30
        )
        
        print(f"      📥 Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            if isinstance(result, list) and len(result) > 0:
                scores = result[0]
                best = max(scores, key=lambda x: x['score'])
                print(f"      ✅ API berhasil: {best['label']} ({best['score']})")
                return best['label'], round(best['score'], 4)
        
        # Jika API gagal, pakai fallback
        print(f"      ⚠️ API gagal (status {response.status_code}), pakai fallback")
        return analyze_sentiment_simple(text)
        
    except Exception as e:
        print(f"      ⚠️ Connection error: {type(e).__name__}, pakai fallback")
        return analyze_sentiment_simple(text)

def run_pipeline():
    print("=" * 60)
    print("🚀 Pipeline: GitHub CSV -> DuckDB -> HuggingFace -> Neon")
    print("=" * 60)

    # TAHAP 0: Buat CSV
    csv_path = "data.csv"
    if not os.path.exists(csv_path):
        print("\n📝 Membuat CSV contoh...")
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
        print(f"\n📂 Menggunakan: {csv_path}")

    # TAHAP 1: Extract
    print("\n📊 TAHAP 1: Extract...")
    conn = duckdb.connect('pipeline.duckdb')
    conn.execute(f"CREATE OR REPLACE TABLE raw_data AS SELECT * FROM read_csv_auto('{csv_path}')")
    row_count = conn.execute("SELECT COUNT(*) FROM raw_data").fetchone()[0]
    print(f"✅ {row_count} baris dibaca.")

    # TAHAP 2: AI Analysis dengan Fallback
    print("\n🤖 TAHAP 2: Sentiment Analysis...")
    hf_token = get_hf_token()
    rows = conn.execute("SELECT id, employee_name, feedback_text, department FROM raw_data").fetchall()
    
    enriched_data = []
    for idx, (emp_id, emp_name, feedback, dept) in enumerate(rows, 1):
        print(f"\n   [{idx}/{len(rows)}] {emp_name}:")
        sentiment, confidence = analyze_sentiment_hf(feedback, hf_token)
        print(f"      ✅ Hasil: {sentiment} (confidence: {confidence})")
        
        enriched_data.append({
            'id': int(emp_id),
            'employee_name': emp_name,
            'feedback_text': feedback,
            'department': dept,
            'sentiment': sentiment,
            'confidence_score': confidence
        })
    
    print(f"\n✅ {len(enriched_data)} feedback dianalisis.")

    # TAHAP 3: Load ke Neon
    print("\n📤 TAHAP 3: Load ke Neon...")
    pipeline_load = dlt.pipeline(
        pipeline_name="github_to_neon_with_ai",
        destination="postgres",
        dataset_name="hr_ai_analytics"
    )
    
    load_info = pipeline_load.run(
        enriched_data,
        table_name="employee_sentiment",
        write_disposition="replace"
    )
    
    print("\n" + "=" * 60)
    print("🎉 BERHASIL!")
    print("=" * 60)
    print(load_info)
    conn.close()

if __name__ == "__main__":
    run_pipeline() 
