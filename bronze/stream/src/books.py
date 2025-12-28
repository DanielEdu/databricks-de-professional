from databricks.sdk.runtime import spark
from stream.read_file import process_bronze


def main():

    topic = "books"
    bad_rec_path = "country/badrecords/"

    schema_value = """
    book_id STRING,
    title STRING,
    author STRING,
    price STRING,
    updated STRING
    """
    
    process_bronze(topic, bad_rec_path, schema_value)


if __name__ == "__main__":
    main()
