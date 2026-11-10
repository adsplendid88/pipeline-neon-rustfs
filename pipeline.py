import dlt
from dlt.sources.filesystem import filesystem

def run_pipeline():
    # ⚠️ GANTI DENGAN BUCKET RUSTFS KAMU YANG SEBENARNYA! ⚠️
    bucket_url = "s3://nama-bucket-rustfs-kamu/retail_orders/"

    data = filesystem(
        bucket_url=bucket_url,
        file_glob="*.csv"
    )

    pipeline = dlt.pipeline(
        pipeline_name="rustfs_to_neon_pipeline",
        destination="postgres",
        dataset_name="retail_data"
    )

    load_info = pipeline.run(data)
    print(load_info)

if __name__ == "__main__":
    run_pipeline() 
