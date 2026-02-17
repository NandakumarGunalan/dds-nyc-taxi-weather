import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm

# ---- CONFIG ----
MONGO_URI = os.environ.get("MONGO_URI")

if not MONGO_URI:
    raise RuntimeError(
        "Missing MONGO_URI environment variable. "
        "Make sure you exported it in your terminal or added it to ~/.zshrc"
    )
DB_NAME = "dds_nyc_taxi_weather"
COLLECTION_NAME = "taxi_trips"
FILE_PATH = "data/raw/yellow_tripdata_2022-01.parquet"
CHUNK_SIZE = 100000  # number of rows per batch

# ---- CONNECT ----
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]
collection.drop()

print("Reading Parquet file...")

df = pd.read_parquet(FILE_PATH)
df = df.head(100_000)
print(f"Total rows: {len(df)}")

# Convert datetime columns to Python datetime (Mongo compatible)
for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
    df[col] = df[col].dt.to_pydatetime()

print("Inserting into MongoDB in batches...")

for start in tqdm(range(0, len(df), CHUNK_SIZE)):
    end = start + CHUNK_SIZE
    batch = df.iloc[start:end].to_dict("records")
    collection.insert_many(batch)

print("Done inserting data!")

