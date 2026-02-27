# Reddit Historical Data — Big Data Analysis Pipeline

> **7.3 million posts** across 14 subreddits · PySpark + DuckDB + VADER · 6 independent analyses

---

## What This Is

A local big data pipeline built to analyse 33GB of Pushshift Reddit data. The goal was to answer real questions about online community behaviour using the same tools used in production data engineering — medallion architecture, columnar storage, distributed query execution — without needing a cloud cluster.

All processing runs locally on an 8GB Windows machine via WSL.

---

## Dataset

| Source | Format | Size |
|--------|--------|------|
| Pushshift Reddit dumps | JSONL | 33 GB raw |
| Posts after cleaning | Parquet | 7,326,016 rows |
| Comments (aitah + fds) | Parquet | 2,154,330 rows |

**Subreddits covered:**

`r/politics` · `r/worldnews` · `r/AITAH` · `r/wallstreetbets` · `r/antiwork` · `r/conservative` · `r/collapse` · `r/unpopularopinion` · `r/trueoffmychest` · `r/changemyview` · `r/femaledatingstrategy` *(banned 2021)* · `r/soccercirclejerk` · `r/formuladank` · `r/dating_advice`

---

## Pipeline Architecture

```
Raw JSONL (33GB)
      │
      ▼
┌─────────────────────────────────────┐
│  BRONZE  — DuckDB                   │
│  fast_bronze.py (~30 min)           │
│  · Schema enforcement               │
│  · Deduplication by post ID         │
│  · [deleted]/[removed] filtered     │
│  · Multi-file merge (wallstreetbets │
│    x3, worldnews x3, politics x2)   │
└─────────────┬───────────────────────┘
              │  Parquet (by subreddit)
              ▼
┌─────────────────────────────────────┐
│  SILVER  — pandas + VADER           │
│  fast_silver_sentiment.py (~60 min) │
│  · VADER sentiment on every title   │
│  · sentiment_label / bucket         │
│  · controversy_ratio                │
│  · text length features             │
└─────────────┬───────────────────────┘
              │  Parquet (enriched)
              ▼
┌─────────────────────────────────────┐
│  ANALYSIS  — PySpark                │
│  6 independent notebooks            │
│  · GroupBy, Window, corr()          │
│  · Self-join, Jaccard similarity    │
│  · MLlib TF-IDF pipeline            │
│  · Lag correlation (contagion)      │
└─────────────────────────────────────┘
```

### Why this tool split?

| Layer | Tool | Why |
|-------|------|-----|
| JSONL → Parquet | DuckDB | Zero JVM overhead, streams 33GB without loading into RAM |
| Row-by-row NLP | pandas | No JVM↔Python serialisation per row — 10–20x faster than Spark UDFs for VADER |
| Aggregations + joins | PySpark | Distributed query planner handles 7M-row self-joins and window functions efficiently |

Spark UDFs serialise every row across the Java↔Python boundary twice. For VADER running on 7M posts, that overhead was the bottleneck — not the NLP itself. Knowing when *not* to use Spark is part of the design.

---

## Results

### 🎣 Analysis 1 — Ragebait Detector

**Metric:** `controversy_ratio = num_comments / score`  
High ratio = community is arguing, not upvoting.

| Subreddit | Controversy Ratio | Tier | Avg Sentiment |
|-----------|------------------|------|---------------|
| r/changemyview | 3.32 | HIGH | -0.061 |
| r/aitah | 1.87 | MEDIUM | -0.078 |
| r/wallstreetbets | 1.39 | MEDIUM | +0.033 |
| r/unpopularopinion | 1.17 | MEDIUM | -0.027 |
| r/trueoffmychest | 0.78 | LOW | -0.142 |
| r/politics | 0.67 | LOW | -0.084 |
| r/femaledatingstrategy | 0.25 | LOW | +0.027 |
| r/formuladank | 0.10 | LOW | +0.047 |

**Key finding:** r/femaledatingstrategy ranks as the *least* controversial subreddit by this metric — lower than meme subs. The community was internally harmonious, not argumentative. This disproves the assumption it was banned for being disruptive and strengthens the content-based ban hypothesis. Also notable: sentiment barely predicts controversy within subreddits. In r/changemyview, negative/neutral/positive posts all generate ~3.3 controversy ratio — people argue regardless of tone.

---

### 🔄 Analysis 2 — Echo Chamber Score

**Metric:** `corr(title_sentiment, upvote_ratio)` per subreddit  
High absolute correlation = community rewards sentiment-aligned posts.

**Sentiment distribution across subreddits:**

| Subreddit | Negative % | Neutral % | Positive % |
|-----------|-----------|-----------|-----------|
| r/worldnews | **52.3%** | 28.2% | 19.5% |
| r/trueoffmychest | **50.8%** | 23.1% | 26.1% |
| r/collapse | **47.3%** | 33.3% | 19.4% |
| r/conservative | **44.8%** | 30.9% | 24.3% |
| r/wallstreetbets | 19.5% | **53.8%** | 26.7% |
| r/formuladank | 17.3% | **57.4%** | 25.4% |

**Key finding:** The upvote-ratio based echo chamber metric returned weak signals across all subreddits — itself a finding. Reddit's vote fuzzing and brigading makes upvote_ratio a noisy signal. The sentiment distribution tells the cleaner story: r/worldnews posts are negative in majority (52.3%) vs r/formuladank at 17.3%, showing fundamentally different community emotional baselines regardless of upvote behaviour. r/politics returned NULL correlation because its upvote_ratio column is always exactly 1.0 — zero variance means no correlation is computable, which reveals another data quality artifact of the Pushshift format.

---

### 🚫 Analysis 3 — Ban Signal Detection *(r/femaledatingstrategy)*

**Method:** Treat the 2021 ban as a known event. Reconstruct 4 signals monthly in the years prior. Compare against r/dating_advice as an unbanned control community.

**Mod removal rate over time:**

| Period | Posts | Removed | Rate |
|--------|-------|---------|------|
| 2019-08 | 183 | 0 | 0.00% |
| 2019-09 | 270 | 1 | 0.37% |
| 2019-11 | 1,178 | 31 | **2.63%** |
| 2019-12 | 1,631 | 84 | **5.15%** |
| 2020-01 | 2,097 | 104 | **4.96%** |
| 2020-02 | 2,394 | 234 | **9.77%** |

**Posting velocity:** 3 posts in Feb 2019 → 2,394 in Feb 2020. +800% year-over-year growth.

**Key finding:** Four measurable signals preceded the ban — posting velocity explosion, mod removal rate spiking from <1% to 9.77%, sentiment extremity increasing, and controversy rising. The combination of hypergrowth + escalating content removal is a quantifiable pre-ban signature. In a production trust & safety pipeline, this pattern would trigger an alert well before admin intervention.

---

### 👤 Analysis 4 — User Crossposting & Audience Overlap

**Method:** Self-join on author column across all subreddits. Jaccard similarity normalises for community size.

**Unique authors per subreddit:**

| Subreddit | Unique Authors |
|-----------|---------------|
| r/worldnews | 396,450 |
| r/politics | 286,722 |
| r/trueoffmychest | 251,634 |
| r/antiwork | 235,331 |
| r/femaledatingstrategy | 12,222 |

**Jaccard similarity — top pairs:**

| Pair | Shared Authors | Jaccard |
|------|---------------|---------|
| politics ↔ worldnews | 52,887 | **0.084** |
| trueoffmychest ↔ unpopularopinion | 13,671 | 0.033 |
| changemyview ↔ unpopularopinion | 4,913 | 0.019 |
| **politics ↔ conservative** | **1,538** | **0.004** |
| wallstreetbets ↔ collapse | 829 | 0.004 |
| femaledatingstrategy ↔ dating_advice | 209 | 0.004 |

**Key finding:** r/politics and r/conservative share fewer than 1,600 users out of a combined 375,000 — a Jaccard similarity of 0.004. This is a measurable filter bubble. Meanwhile r/politics and r/worldnews share 52,887 users (Jaccard 0.084) — they are functionally the same community. The ideological pairs (politics/conservative, fds/dating_advice, wsb/collapse) all cluster at the same near-zero Jaccard, suggesting ideologically opposed communities have essentially no audience overlap regardless of topic.

**Bridge users:** 15,285 users posted across 3 or more subreddits. The most cross-cutting users spanned 8 subreddits simultaneously — including conservative, collapse, wallstreetbets, politics, and worldnews. These are the community connectors the filter bubble literature talks about.

---

### 🌊 Analysis 5 — Sentiment Contagion (worldnews → politics)

**Method:** Compute daily average sentiment per subreddit. Use `lag()` window function to shift worldnews sentiment by 1/2/3 days. Measure correlation with each target sub at each lag.

**Correlation results:**

| Target | Same-day | Lag-1 | Lag-2 | Finding |
|--------|---------|-------|-------|---------|
| r/politics | 0.162 | 0.129 | 0.125 | No lag advantage |
| **r/conservative** | **0.349** | 0.319 | 0.313 | No lag advantage |
| r/collapse | 0.078 | 0.016 | 0.032 | No contagion |

**Key finding:** No subreddit showed a lag advantage — same-day correlation was always strongest. worldnews and conservative correlate at 0.35 on the same day, meaning they react to the same news cycle simultaneously rather than one influencing the other. The stronger correlation with conservative than with politics is the most interesting result: conservative Reddit is more tightly coupled to international news framing than the explicitly political subreddit.

**Event validation:** The most negative day in the entire 7.3M post dataset is **2015-11-14** — the day after the Paris attacks — with a worldnews average sentiment of -0.341. Identified with no labels, purely from the data.

---

### 📈 Analysis 6 — Topic Drift Over Time (TF-IDF)

**Method:** Spark MLlib TF-IDF pipeline. Each (subreddit × quarter) treated as a single document. Extracts the most distinctive vocabulary per community per time period.

**Pipeline:** `Tokenizer → StopWordsRemover → HashingTF → IDF`

- 427 documents (subreddit × quarter combinations)
- Reddit-specific stopwords added on top of standard English
- Top 500 posts by score sampled per quarter to avoid OOM on 8GB RAM

Quarterly topic fingerprints show vocabulary shifts that map to real-world events — r/wallstreetbets GME vocabulary spiking in early 2021, r/collapse language shifting across crisis cycles, r/politics vocabulary changing with election years.

---

## Stack

```
Python 3.10
PySpark 3.5.1
DuckDB 0.10+
pandas 2.x
pyarrow
vaderSentiment
Spark MLlib (TF-IDF)
Jupyter Notebook
WSL2 (Ubuntu) on Windows
```

---

## Setup & Run

### Prerequisites
- Java 17 (required by Spark)
- Python 3.10+
- WSL2 recommended on Windows

```bash
pip install duckdb pandas pyarrow vaderSentiment pyspark==3.5.1 jupyter
```

### Data
Place Pushshift JSONL files in `data/bronze/raw/`. Multiple files per subreddit are handled automatically — they are merged and deduplicated by post ID.

### Run order

```bash
# Step 1 — JSONL → clean Parquet (~30 min for 33GB)
python fast_bronze.py

# Step 2 — Add VADER sentiment to every post (~60–90 min)
python fast_silver_sentiment.py

# Step 3 — Open analysis notebooks
jupyter notebook
```

Notebooks 02–07 are fully independent and can be run in any order after Step 2 completes.

---

## Repo Structure

```
├── fast_bronze.py              # DuckDB ingestion + dedup
├── fast_silver_sentiment.py    # VADER sentiment enrichment
├── notebooks/
│   ├── 02_ragebait_detector.ipynb
│   ├── 03_echo_chamber.ipynb
│   ├── 04_ban_signal.ipynb
│   ├── 05_crossposting.ipynb
│   ├── 06_topic_drift.ipynb
│   └── 07_sentiment_contagion.ipynb
├── data/
│   ├── bronze/raw/             # Raw JSONL input (not committed)
│   ├── silver/posts_sentiment/ # Enriched Parquet (not committed)
│   └── gold/                   # Aggregated outputs (not committed)
└── README.md
```

---

## Design Decisions Worth Noting

**Why not Spark for everything?**  
Spark Python UDFs serialise every row across the JVM↔Python boundary twice per call. For VADER running row-by-row on 7M posts, that overhead dominates. pandas processes the same rows in a tight Python loop with no boundary — roughly 10–20x faster for this specific workload. Spark's strength is distributed aggregation and joins, not row-level Python NLP.

**Why DuckDB for ingestion?**  
DuckDB reads JSONL natively, streams files without loading them fully into RAM, and writes Parquet in a single SQL statement. It processed 33GB in under 35 minutes. The equivalent Spark job took 8–9 hours on the same machine due to JVM startup, shuffle overhead, and partition management on a single node.

**Why medallion architecture locally?**  
Separating Bronze (raw), Silver (enriched), and Gold (aggregated) means any analysis can be rerun or modified without reprocessing upstream data. Adding a 7th analysis notebook costs nothing — it just reads from Silver. Changing the sentiment model would only require rerunning `fast_silver_sentiment.py`.
