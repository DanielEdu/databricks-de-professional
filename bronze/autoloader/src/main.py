from databricks.sdk.runtime import spark
from autoloader.read_file import process_bronze


def main():
    process_bronze("bronze.default.country_brz")


if __name__ == "__main__":
    main()
