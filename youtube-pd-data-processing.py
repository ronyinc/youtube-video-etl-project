import os
import pandas as pd

# 1) Read input CSV
input_path = "./landing-input/youtube_data_2025-10-09.csv"
df = pd.read_csv(input_path, dtype=str)  # keep as strings to avoid surprises

# 2) Keep only ISO-8601 time-only durations that start with "PT"
df = df[df["duration"].str.startswith("PT", na=False)].copy()

# 3) Extract H / M / S (each is optional)
#    These patterns work whether it's PT11H48M40S, PT3M14S, PT34S, etc.
df["hours"]   = df["duration"].str.extract(r"(\d+)H", expand=False)
df["minutes"] = df["duration"].str.extract(r"(\d+)M", expand=False)
df["seconds"] = df["duration"].str.extract(r"(\d+)S", expand=False)

# 4) Fill missing with 0 and convert to integers
for c in ["hours", "minutes", "seconds"]:
    df[c] = df[c].fillna(0).astype(int)

# 5) Compute total seconds
df["total_seconds"] = df["hours"] * 3600 + df["minutes"] * 60 + df["seconds"]

# (Optional) choose/ordered columns for output if you like
cols = [c for c in ["video_id","title","publishedAt","duration","hours","minutes","seconds","total_seconds"] if c in df.columns]
df_out = df[cols] if cols else df

# 6) Write to a single CSV file
output_dir = "./staging_files"
os.makedirs(output_dir, exist_ok=True)
out_path = os.path.join(output_dir, "youtube-staged.csv")

df_out.to_csv(out_path, index=False, encoding="utf-8", lineterminator="\n")
print(f"Wrote CSV to {os.path.abspath(out_path)}")
