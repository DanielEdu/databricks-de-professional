from databricks.sdk.runtime import spark
from autoloader.read_file import process_bronze


def main():
    target = "bronze.bookstore.country_brz"
    path = "country/data/"
    chk_path = "checkpoint/country/"
    bad_rec_path = "country/badrecords/"
    schema = "code STRING, calling_code STRING, country STRING"
    
    process_bronze(target, path, chk_path, bad_rec_path, schema)


if __name__ == "__main__":
    main()
