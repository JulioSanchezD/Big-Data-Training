import pandas
import fastavro

def avro_df(filepath, encoding):
    # Open file stream
    with open(filepath, 'rb') as fp:
        # Configure Avro reader
        reader = fastavro.reader(fp)
        # Load records in memory
        records = [r for r in reader]
        # Populate pandas.DataFrame with records
        df = pandas.DataFrame.from_records(records)
        # Return created DataFrame
        return df

df = avro_df("data/test-dataset.avro", "utf-8")
df.to_csv("data/test.csv", index=False)