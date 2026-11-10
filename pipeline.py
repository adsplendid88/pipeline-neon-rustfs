import dlt
from dlt.sources.filesystem import filesystem, read_csv

def run_pipeline():
    # 1. Definisikan sumber dengan filter incremental
    source = filesystem(
        file_glob="**/*.csv",
        incremental=dlt.sources.incremental("modification_date"),
    )

    # 2. Pipe ke reader CSV dan beri nama tabel tujuan di Neon
    table = (source | read_csv(encoding="latin-1")).with_name("retail_orders")

    # 3. Konfigurasi pipeline
    pipeline = dlt.pipeline(
        pipeline_name="rustfs_to_neon",
        destination="postgres",
        dataset_name="retail_data",
    )

    # 4. Jalankan
    load_info = pipeline.run(table)
    print(load_info)

if __name__ == "__main__":
    run_pipeline()
