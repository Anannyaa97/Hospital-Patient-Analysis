"""
Sample the large SPARCS CSV down to a manageable size for local processing.

Run with:
    python sample_dataset.py

Reads the full hospital_discharge.csv in chunks (so it never loads the whole
983MB file into memory at once) and writes out a random sample of 300,000
rows to a new, smaller file.
"""

import pandas as pd

INPUT_PATH = "data/raw/hospital_discharge.csv"
OUTPUT_PATH = "data/raw/hospital_discharge_sample.csv"
SAMPLE_ROWS = 300_000
CHUNK_SIZE = 100_000

print("Reading and sampling in chunks (this may take a minute or two)...")

chunks_written = 0
rows_written = 0
first_chunk = True

# Roughly 300,000 / total_rows fraction — we don't know total rows in advance,
# so we sample a fraction of each chunk instead. For a ~2M row file, ~15% per
# chunk gets us close to 300k rows total.
SAMPLE_FRAC = 0.15

for chunk in pd.read_csv(INPUT_PATH, chunksize=CHUNK_SIZE, low_memory=False):
    sampled = chunk.sample(frac=SAMPLE_FRAC, random_state=42)
    sampled.to_csv(
        OUTPUT_PATH,
        mode="w" if first_chunk else "a",
        header=first_chunk,
        index=False
    )
    rows_written += len(sampled)
    chunks_written += 1
    first_chunk = False
    print(f"  Processed chunk {chunks_written}, total sampled rows so far: {rows_written}")

print(f"\nDone. Sampled file written to {OUTPUT_PATH} with {rows_written} rows.")
