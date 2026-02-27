#!/bin/bash
# Run from your project folder: bash write_bronze.sh

cat > fast_bronze.py << 'PYEOF'
import duckdb, os, glob, time, re
from pathlib import Path
from collections import defaultdict

RAW_DIR      = "data/bronze/raw"
POSTS_OUT    = "data/silver/posts"
COMMENTS_OUT = "data/silver/comments"
TEMP_DIR     = "data/bronze/temp"

for d in [POSTS_OUT, COMMENTS_OUT, TEMP_DIR]:
    os.makedirs(d, exist_ok=True)

con = duckdb.connect()
con.execute("SET memory_limit='5GB'")
con.execute("SET threads=4")
con.execute(f"SET temp_directory='{TEMP_DIR}'")

all_files   = glob.glob(f"{RAW_DIR}/**/*.jsonl", recursive=True)
jsonl_files = [f for f in all_files if os.path.getsize(f) > 1024]
if not jsonl_files:
    print(f"No .jsonl files found in {RAW_DIR}/"); exit(1)

def extract_subreddit(filepath):
    name = Path(filepath).stem.lower()
    name = re.sub(r'^r[_/]', '', name)
    name = re.sub(r'_(posts|comments|submissions).*$', '', name)
    name = re.sub(r'_\d{4}.*$', '', name)
    return name.strip('_')

def is_comments_file(filepath):
    return '_comments' in Path(filepath).stem.lower()

groups = defaultdict(lambda: {'posts': [], 'comments': []})
for f in jsonl_files:
    sub  = extract_subreddit(f)
    kind = 'comments' if is_comments_file(f) else 'posts'
    groups[sub][kind].append(f)
    print(f"  [{kind:8s}] {sub:35s} <- {Path(f).name}  ({os.path.getsize(f)/1024/1024:.0f} MB)")

print(f"\nSubreddits: {sorted(groups.keys())}\n")

def get_columns(con, files_sql):
    """Detect which columns actually exist in this file."""
    try:
        row = con.execute(f"""
            SELECT * FROM read_json_auto(
                {files_sql},
                format='newline_delimited',
                ignore_errors=true,
                maximum_object_size=10485760,
                union_by_name=true
            ) LIMIT 1
        """).description
        return [d[0].lower() for d in row]
    except:
        return []

def safe_col(existing_cols, col_name, cast_type, default, alias):
    """Return SQL for a column that may or may not exist in the source."""
    if col_name.lower() in existing_cols:
        if default is None:
            return f"    TRY_CAST({col_name} AS {cast_type}) AS {alias}"
        else:
            return f"    COALESCE(TRY_CAST({col_name} AS {cast_type}), {default}) AS {alias}"
    else:
        # Column doesn't exist — return the default value directly
        if default is None:
            return f"    NULL AS {alias}"
        else:
            return f"    {default} AS {alias}"

def build_posts_sql(files_sql, existing_cols):
    parts = [
        "    COALESCE(CAST(id AS VARCHAR), '')                      AS id",
        "    ,lower(trim(COALESCE(CAST(subreddit AS VARCHAR),'')))  AS subreddit",
        "    ,lower(trim(COALESCE(CAST(author AS VARCHAR),'[unknown]'))) AS author",
        "    ,COALESCE(CAST(title AS VARCHAR),'')                   AS title",
        "    ,COALESCE(CAST(selftext AS VARCHAR),'')                AS selftext",
        "    ,trim(COALESCE(CAST(title AS VARCHAR),'') || ' ' || COALESCE(CAST(selftext AS VARCHAR),'')) AS full_text",
        "    ,COALESCE(TRY_CAST(score AS INTEGER),0)                AS score",
        "    ,COALESCE(TRY_CAST(num_comments AS INTEGER),0)         AS num_comments",
        "    ,TRY_CAST(created_utc AS BIGINT)                       AS created_utc",
        "    ,COALESCE(CAST(domain AS VARCHAR),'self')              AS domain",
        "    ,COALESCE(TRY_CAST(is_self AS BOOLEAN),true)           AS is_self",
        "    ,COALESCE(TRY_CAST(over_18 AS BOOLEAN),false)          AS over_18",
        "    ,COALESCE(TRY_CAST(stickied AS BOOLEAN),false)         AS stickied",
        "    ,to_timestamp(TRY_CAST(created_utc AS BIGINT))         AS created_ts",
        "    ,strftime(to_timestamp(TRY_CAST(created_utc AS BIGINT)),'%Y')    AS year",
        "    ,strftime(to_timestamp(TRY_CAST(created_utc AS BIGINT)),'%m')    AS month",
        "    ,strftime(to_timestamp(TRY_CAST(created_utc AS BIGINT)),'%Y-%m') AS year_month",
        "    ,strftime(to_timestamp(TRY_CAST(created_utc AS BIGINT)),'%Y') || '-Q' || CAST(((CAST(strftime(to_timestamp(TRY_CAST(created_utc AS BIGINT)),'%m') AS INT)-1)/3)+1 AS VARCHAR) AS quarter",
        "    ,EXTRACT(hour FROM to_timestamp(TRY_CAST(created_utc AS BIGINT))) AS hour_of_day",
        "    ,EXTRACT(dow  FROM to_timestamp(TRY_CAST(created_utc AS BIGINT))) AS day_of_week",
        "    ,COALESCE(TRY_CAST(score AS INTEGER),0)+(COALESCE(TRY_CAST(num_comments AS INTEGER),0)*2) AS engagement_score",
        "    ,CASE WHEN COALESCE(TRY_CAST(score AS INTEGER),0)>0 THEN COALESCE(TRY_CAST(num_comments AS INTEGER),0)::DOUBLE/COALESCE(TRY_CAST(score AS INTEGER),0)::DOUBLE ELSE COALESCE(TRY_CAST(num_comments AS INTEGER),0)::DOUBLE END AS controversy_ratio",
    ]
    # Optional columns — safely handled
    parts.append("    ," + safe_col(existing_cols, 'upvote_ratio',       'DOUBLE',  '0.0',   'upvote_ratio').strip())
    parts.append("    ," + safe_col(existing_cols, 'locked',             'BOOLEAN', 'false', 'locked').strip())
    parts.append("    ," + safe_col(existing_cols, 'removed_by_category','VARCHAR', 'NULL',  'removed_by_category').strip())
    parts.append("    ," + safe_col(existing_cols, 'link_flair_text',    'VARCHAR', 'NULL',  'flair').strip())
    parts.append("    ," + safe_col(existing_cols, 'total_awards_received','INTEGER','0',    'total_awards').strip())
    # is_removed derived from removed_by_category
    if 'removed_by_category' in existing_cols:
        parts.append("    ,(TRY_CAST(removed_by_category AS VARCHAR) IS NOT NULL) AS is_removed")
    else:
        parts.append("    ,false AS is_removed")
    return "\n".join(parts)

def build_comments_sql(existing_cols):
    parts = [
        "    COALESCE(CAST(id AS VARCHAR),'')                           AS id",
        "    ,COALESCE(CAST(link_id AS VARCHAR),'')                     AS post_id",
        "    ,COALESCE(CAST(parent_id AS VARCHAR),'')                   AS parent_id",
        "    ,lower(trim(COALESCE(CAST(subreddit AS VARCHAR),'')))      AS subreddit",
        "    ,lower(trim(COALESCE(CAST(author AS VARCHAR),'[unknown]'))) AS author",
        "    ,COALESCE(CAST(body AS VARCHAR),'')                        AS body",
        "    ,COALESCE(TRY_CAST(score AS INTEGER),0)                    AS score",
        "    ,TRY_CAST(created_utc AS BIGINT)                           AS created_utc",
        "    ,COALESCE(TRY_CAST(controversiality AS INTEGER),0)         AS controversiality",
        "    ,to_timestamp(TRY_CAST(created_utc AS BIGINT))             AS created_ts",
        "    ,strftime(to_timestamp(TRY_CAST(created_utc AS BIGINT)),'%Y')    AS year",
        "    ,strftime(to_timestamp(TRY_CAST(created_utc AS BIGINT)),'%Y-%m') AS year_month",
    ]
    parts.append("    ," + safe_col(existing_cols, 'removed_by_category','VARCHAR','NULL','removed_by_category').strip())
    return "\n".join(parts)

def process_files(subreddit, file_list, out_dir, kind):
    out_path = f"{out_dir}/{subreddit}.parquet"
    if os.path.exists(out_path):
        n = con.execute(f"SELECT COUNT(*) FROM '{out_path}'").fetchone()[0]
        print(f"  SKIP r/{subreddit} [{kind}] — already done ({n:,} rows)")
        return n

    start = time.time()
    text_col = "selftext" if kind == "posts" else "body"

    if len(file_list) == 1:
        files_sql = f"'{file_list[0]}'"
    else:
        files_sql = "[" + ", ".join(f"'{f}'" for f in file_list) + "]"
        print(f"  Merging {len(file_list)} files for r/{subreddit}")

    # Detect columns that actually exist in this file
    existing_cols = get_columns(con, files_sql)
    print(f"  Detected {len(existing_cols)} columns in {subreddit} [{kind}]")

    if kind == "posts":
        col_sql = build_posts_sql(files_sql, existing_cols)
    else:
        col_sql = build_comments_sql(existing_cols)

    try:
        con.execute(f"""
            COPY (
                SELECT * EXCLUDE (_rn)
                FROM (
                    SELECT
                        {col_sql},
                        ROW_NUMBER() OVER (
                            PARTITION BY id
                            ORDER BY COALESCE(TRY_CAST(score AS INTEGER),0) DESC
                        ) AS _rn
                    FROM read_json_auto(
                        {files_sql},
                        format='newline_delimited',
                        ignore_errors=true,
                        maximum_object_size=10485760,
                        union_by_name=true
                    )
                    WHERE id IS NOT NULL
                      AND CAST(id AS VARCHAR) != ''
                      AND TRY_CAST(created_utc AS BIGINT) IS NOT NULL
                      AND COALESCE(CAST({text_col} AS VARCHAR),'') NOT IN ('[deleted]','[removed]')
                ) t
                WHERE _rn = 1
            )
            TO '{out_path}'
            (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000)
        """)
        count   = con.execute(f"SELECT COUNT(*) FROM '{out_path}'").fetchone()[0]
        size_mb = os.path.getsize(out_path) / 1024 / 1024
        print(f"  OK  r/{subreddit:30s} [{kind:8s}]  {count:>9,} rows  {size_mb:6.0f} MB  {time.time()-start:.0f}s")
        return count
    except Exception as e:
        print(f"  ERR r/{subreddit} [{kind}]: {e}")
        return 0

all_start = time.time()
tp = tc = 0

print("="*65 + "\nPOSTS\n" + "="*65)
for sub, files in sorted(groups.items()):
    if files['posts']:
        tp += process_files(sub, files['posts'], POSTS_OUT, 'posts')

print("\n" + "="*65 + "\nCOMMENTS\n" + "="*65)
for sub, files in sorted(groups.items()):
    if files['comments']:
        tc += process_files(sub, files['comments'], COMMENTS_OUT, 'comments')

print(f"\nDone in {(time.time()-all_start)/60:.1f} min")
print(f"  Posts    : {tp:,}  ->  {POSTS_OUT}/")
print(f"  Comments : {tc:,}  ->  {COMMENTS_OUT}/")
print(f"\nNext: python fast_silver_sentiment.py")
PYEOF

echo "fast_bronze.py created successfully"
wc -l fast_bronze.py
