"""
fast_silver_sentiment.py
------------------------
Replaces 01_silver_nlp.ipynb

Adds VADER sentiment to every post using plain Python + pandas in chunks.
No Spark needed — pandas processes one Parquet file at a time, so memory stays flat.

On 20GB → ~30-60 min depending on number of posts.
Spark equivalent: 8-9 hours (due to UDF serialisation overhead).

Install: pip install vaderSentiment pandas pyarrow fastparquet

Usage:
    python scripts/fast_silver_sentiment.py
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import glob, os, time
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
IN_DIR   = "data/silver/posts"            # output from fast_bronze.py
OUT_DIR  = "data/silver/posts_sentiment"  # parquet files with sentiment added
CHUNK    = 50_000                          # rows per chunk — fits in ~500MB RAM

os.makedirs(OUT_DIR, exist_ok=True)

# ── Init VADER once ───────────────────────────────────────────────────────────
# Creating SentimentIntensityAnalyzer is expensive (~0.5s).
# We create it once and reuse for all rows.
vader = SentimentIntensityAnalyzer()

def score_text(text: str) -> float:
    """Return VADER compound score. Cap at 512 chars — longer text doesn't help."""
    if not text or not isinstance(text, str):
        return 0.0
    return vader.polarity_scores(text[:512])["compound"]

def sentiment_label(score: float) -> str:
    if score >= 0.05:
        return "positive"
    if score <= -0.05:
        return "negative"
    return "neutral"

def sentiment_bucket(score: float) -> str:
    if score >= 0.5:   return "strongly_positive"
    if score >= 0.05:  return "mildly_positive"
    if score <= -0.5:  return "strongly_negative"
    if score <= -0.05: return "mildly_negative"
    return "neutral"

# ── Process each parquet file ─────────────────────────────────────────────────
parquet_files = sorted(glob.glob(f"{IN_DIR}/*.parquet"))

if not parquet_files:
    print(f"No parquet files found in {IN_DIR}")
    print("Run fast_bronze.py first.")
    exit(1)

print(f"Found {len(parquet_files)} parquet files to process\n")

all_start = time.time()
total_rows = 0

for fpath in parquet_files:
    fname = Path(fpath).stem
    out_path = f"{OUT_DIR}/{fname}_sentiment.parquet"

    # Skip if already done (resume support)
    if os.path.exists(out_path):
        print(f"  SKIP (already done): {fname}")
        continue

    file_start = time.time()
    print(f"Processing: {fname}")

    # Read file in chunks using PyArrow (memory efficient)
    pf = pq.ParquetFile(fpath)
    chunks_out = []

    for batch_num, batch in enumerate(pf.iter_batches(batch_size=CHUNK)):
        df = batch.to_pandas()

        # ── Sentiment scoring (vectorised apply) ──────────────────────────────
        # pandas .apply() is row-by-row Python — faster than Spark UDF because
        # no serialisation overhead between JVM and Python
        titles = df["title"].fillna("").astype(str)
        fulltexts = df["full_text"].fillna("").astype(str)

        df["title_sentiment"]    = titles.apply(score_text)
        df["fulltext_sentiment"] = fulltexts.apply(score_text)
        df["sentiment_label"]    = df["title_sentiment"].apply(sentiment_label)
        df["sentiment_bucket"]   = df["title_sentiment"].apply(sentiment_bucket)

        # ── Text length features ──────────────────────────────────────────────
        df["title_word_count"]    = titles.str.split().str.len().fillna(0).astype(int)
        df["selftext_word_count"] = (
            df["selftext"].fillna("").astype(str)
            .str.split().str.len().fillna(0).astype(int)
        )
        df["has_body"] = df["selftext_word_count"] > 10

        # ── Momentum / engagement ─────────────────────────────────────────────
        df["score_per_comment"] = (
            df["score"].where(df["num_comments"] > 0, other=0).astype(float) /
            df["num_comments"].replace(0, 1).astype(float)
        )

        chunks_out.append(df)

        rows_done = (batch_num + 1) * CHUNK
        print(f"  chunk {batch_num+1}: {min(rows_done, len(df)):,} rows scored", end="\r")

    # Concatenate all chunks and write
    result = pd.concat(chunks_out, ignore_index=True)
    result.to_parquet(out_path, index=False, compression="snappy")

    elapsed = time.time() - file_start
    size_mb = os.path.getsize(out_path) / 1024 / 1024
    total_rows += len(result)
    print(f"\n  ✓ {len(result):,} rows → {out_path} ({size_mb:.0f} MB) in {elapsed:.0f}s\n")

total_elapsed = time.time() - all_start
print(f"{'='*60}")
print(f"Done. {total_rows:,} rows with sentiment in {total_elapsed/60:.1f} minutes")
print(f"Output: {OUT_DIR}/")
print(f"\nNext: open any analysis notebook and point it at {OUT_DIR}/")
