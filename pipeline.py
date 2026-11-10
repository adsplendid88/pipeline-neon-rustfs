import dlt
from dlt.sources.filesystem import filesystem

def run_pipeline():
    # ⚠️ PENTING: GANTI URL DI BAWAH INI DENGAN BUCKET RUSTFS KAMU YANG SEBENARNYA! ⚠️
    # Formatnya harus: s3://<nama-bucket-kamu>/<folder-data>/
    # Contoh: "s3://my-rustfs-bucket/retail_orders/"
    bucket_url = "s3://nama-bucket-rustfs-kamu/retail_orders/" 

    # Inisialisasi resource filesystem
    # Ganti "*.csv" jika file kamu berformat lain (misal: "*.json" atau "*.parquet")
    data = filesystem(
        bucket_url=bucket_url,
        file_glob="*.csv" 
    )

    # Inisialisasi pipeline dlt ke Neon (Postgres)
    pipeline = dlt.pipeline(
        pipeline_name="rustfs_to_neon_pipeline",
        destination="postgres",
        dataset_name="retail_data"
    )

    # Jalankan pipeline (extract data dari RustFS & load ke Neon)
    load_info = pipeline.run(data)
    
    # Print hasil eksekusi
    print(load_info)

if __name__ == "__main__":
    run_pipeline() 

    # 4. Jalankan
    load_info = pipeline.run(table)
    print(load_info)

if __name__ == "__main__":
    run_pipeline()
