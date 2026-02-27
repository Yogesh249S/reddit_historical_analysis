# 🔬 Reddit Big Data Analysis Suite
### PySpark analysis of 20GB+ Pushshift Reddit data

---

## Setup

```bash
pip install pyspark==3.5.1 vaderSentiment jupyter notebook
```

Java 17 required — see the lakehouse README if not installed.

---

## Data Setup

Place your `.jsonl` files in `data/bronze/raw/`:
```
data/bronze/raw/
  politics.jsonl
  worldnews.jsonl
  AITAH.jsonl
  wallstreetbets.jsonl
  femaledatingstrategy.jsonl
  ...
```

---

## Run Order

| # | Notebook | What it does | Est. time (20GB) |
|---|----------|-------------|-----------------|
| 00 | `00_bronze_ingest.ipynb` | JSONL → Parquet, filter deleted posts | 20–40 min |
| 01 | `01_silver_nlp.ipynb` | Clean + VADER sentiment on every post | 30–60 min |
| 02 | `02_ragebait_detector.ipynb` | Controversy ratio analysis | 3 min |
| 03 | `03_echo_chamber.ipynb` | Sentiment×upvote correlation | 3 min |
| 04 | `04_ban_signal.ipynb` | Pre-ban signal detection (FDS) | 4 min |
| 05 | `05_crossposting.ipynb` | User overlap + Jaccard similarity | 5 min |
| 06 | `06_topic_drift.ipynb` | TF-IDF topic fingerprints over time | 10 min |
| 07 | `07_sentiment_contagion.ipynb` | worldnews lag correlation | 4 min |

> Notebooks 00 and 01 are slow (processing 20GB). Run them overnight or on a weekend.  
> Notebooks 02–07 all read from Silver and are fast.

---

## Interview Talking Points

### Overall
> "I ran 6 distinct analyses on 20GB of Pushshift Reddit data using PySpark locally — covering NLP, graph analysis, time-series lag correlation, and unsupervised topic modelling. Every analysis is a separate notebook reading from a shared Silver layer so adding a new analysis doesn't require reprocessing."

### On the Silver layer design
> "I centralised VADER sentiment scoring in the Silver layer as a UDF. That means I run it once on 20GB of text, pay the cost once, and every downstream analysis notebook gets sentiment for free. If I'd done it per-analysis, I'd be re-running VADER 6 times."

### On UDFs vs built-in functions
> "VADER runs as a Python UDF — it processes rows one at a time in Python, which is slower than Spark's native JVM functions. For production I'd switch to a vectorised Pandas UDF which batches rows into DataFrames, typically 5–10x faster. But for local 20GB analysis, regular UDFs are fine."

### On the self-join (crossposting)
> "The crossposting analysis uses a self-join on the author column — the same DataFrame joined to itself with different aliases. The key trick is `sub_a < sub_b` to avoid counting each pair twice. With 20GB of data, this join shuffles a lot of data so I cache the deduplicated author-subreddit pairs first."

### On lag correlation (contagion)
> "The contagion analysis uses Spark's `lag()` window function — the same pattern as SQL's LAG(). I compute daily sentiment per subreddit, then shift worldnews by 1/2/3 days and correlate with next-day politics sentiment. If the 1-day lag correlates more strongly than same-day, you've found a leading indicator."

### On TF-IDF
> "I used Spark MLlib's TF-IDF pipeline — Tokenizer → StopWordsRemover → HashingTF → IDF. The key insight is treating each (subreddit, quarter) as a single 'document' by concatenating all post titles. That lets you find terms that are distinctive to a subreddit in a given period vs all other periods."

### On the ban signal
> "I treated the FDS ban as a known event and did backwards analysis — computing four signals monthly before the ban date: posting velocity, sentiment extremity, removal rate, and controversy ratio. I combined them into a composite score. This is the anomaly detection pattern trust & safety teams use at scale."
