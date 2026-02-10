# Actuarial Reddit Product Insights

Scrapes r/actuary Reddit posts and uses a local LLM (Ollama/llama3) to extract product insights — emotions, personas, needs, and solution gaps — for designing an actuarial exam education product. Results are stored in DuckDB and explored via `duckdb -ui`.

## Why

Reddit's r/actuary is full of exam candidates sharing struggles, study habits, and unmet needs. This project turns those posts into structured product research: who the users are, what they feel, what they need, and how well existing solutions serve them.

## Pipeline

```
Reddit API (PRAW)  →  DuckDB (raw posts)  →  Ollama/llama3 (LLM analysis)  →  DuckDB (structured insights)
```

1. **Scrape** — `data/raw/reddit.py` pulls up to 2,000 recent posts from r/actuary via PRAW, including all comments. Saves to CSV and loads into DuckDB.
2. **Analyze** — `models/reddit_analysis.py` registers three LLM-powered UDFs in DuckDB and runs them over every post:
   - `text_analysis` — extracts **emotion**, **situation**, **action**, and **intent**
   - `persona_analysis` — infers **persona**, **study scenario**, and **need statement**
   - `solution_analysis` — scores how well existing alternatives address each need (1–5 scale)
3. **Explore** — Results land in DuckDB tables you can query directly or browse with `duckdb -ui`.

## Key Tables

| Table | Description |
|---|---|
| `reddit_posts` | Raw posts: title, selftext, comments, score, author, timestamps |
| `reddit_analysis` | Posts joined with emotion/situation/action/intent from the LLM |
| `reddit_persona_analysis` | Persona, study scenario, and need statement per post |
| `silver.reddit_llm` | Clean, flattened table combining persona + text analysis fields |
| `reddit_solution_analysis` | Alternative solution descriptions and scores (1–5) per need |

## Setup

### Prerequisites

- Python 3.10+
- [Ollama](https://ollama.com) with `llama3` and `llama3.1` models pulled
- A Reddit API app (for PRAW credentials)

### Install

```bash
pip install praw duckdb pandas ollama pydantic python-dotenv
```

### Configure

Create a `.env` file in the project root:

```
REDDIT_CLIENT_ID=your_client_id
REDDIT_SECRET=your_secret
user_name=your_reddit_username
```

### Run

```bash
# 1. Pull Ollama models
ollama pull llama3
ollama pull llama3.1

# 2. Scrape Reddit posts
python data/raw/reddit.py

# 3. Run LLM analysis (takes a while — processes every post through Ollama)
python models/reddit_analysis.py

# 4. Explore results
duckdb data/raw/reddit.db -ui
```


## Demo

![DuckDB Analysis Result](duckdb_analysis_result.png)

## Project Structure

```
data/raw/
  reddit.py              # Scrapes r/actuary via PRAW, loads into DuckDB
  reddit.db              # DuckDB database with all tables
  reddit_posts.csv       # Raw scraped posts

models/
  reddit_analysis.py     # LLM analysis pipeline (Ollama UDFs in DuckDB)
  reddit_analysis_results.csv
```