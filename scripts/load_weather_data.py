import os
import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm

# ---- CONFIG ----
DB_NAME = "dds_nyc_taxi_weather"
COLLECTION_NAME = "weather_daily"

# If your csv name differs, update this:
FILE_PATH = "data/raw/NYC_Central_Park_weather_1869-2022.csv"

# Optional: keep only 2022 to match taxi month (saves space + simpler joins)
KEEP_ONLY_2022 = True

# Mongo URI should come from env var (recommended)
MONGO_URI = os.environ.get("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("Missing MONGO_URI env var. Example: export MONGO_URI='mongodb+srv://...'")

# ---- CONNECT ----
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Drop so reruns are clean (optional but convenient during dev)
collection.drop()

print("Reading weather CSV...")
df = pd.read_csv(FILE_PATH)

# scripts/load_weather_data.py

import os
import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm

# ---- CONFIG ----
DB_NAME = "dds_nyc_taxi_weather"
COLLECTION_NAME = "weather_daily"
FILE_PATH = "data/raw/weather_2022.csv"   # <-- make sure this exists
CHUNK_SIZE = 10000                        # safe for CSV; weather is small anyway

# ---- MONGO URI from env ----
MONGO_URI = os.environ.get("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError(
        "Missing MONGO_URI env var. Example:\n"
        "export MONGO_URI='mongodb+srv://<user>:<pass>@<cluster>/?retryWrites=true&w=majority'"
    )

# ---- CONNECT ----
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Drop collection so reruns don't duplicate data
collection.drop()

print("Reading weather CSV...")
df = pd.read_csv(FILE_PATH)

# Normalize column names (trim + uppercase)
df.columns = [c.strip().upper() for c in df.columns]

# Ensure DATE is datetime (Mongo-friendly)
if "DATE" not in df.columns:
    raise ValueError(f"DATE column not found. Columns are: {list(df.columns)}")

df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
df = df.dropna(subset=["DATE"])

# Keep only expected columns if present
keep = ["DATE", "PRCP", "SNOW", "SNWD", "TMIN", "TMAX"]
existing = [c for c in keep if c in df.columns]
df = df[existing]

# Convert pandas Timestamp -> python datetime
df["DATE"] = df["DATE"].dt.to_pydatetime()

records = df.to_dict("records")
print(f"Rows to insert: {len(records)}")
print("Inserting into MongoDB...")

# Insert in batches (even though small)
for i in tqdm(range(0, len(records), CHUNK_SIZE)):
    batch = records[i : i + CHUNK_SIZE]
    if batch:
        collection.insert_many(batch)

# Helpful indexes for joins/filters later
collection.create_index("DATE")

print(f"Done. Collection: {DB_NAME}.{COLLECTION_NAME}")

