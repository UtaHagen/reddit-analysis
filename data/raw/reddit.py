import requests
import praw
from datetime import datetime, timedelta
import time
import dotenv
import os

dotenv.load_dotenv()

# Initialize Reddit API
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_SECRET"),
    user_agent=f"my scraper by u/{os.getenv('user_name')}",
)

post_list = []
idx = 0
failed_posts = []
# Choose a subreddit
subreddit = reddit.subreddit("actuary")  # You can change this
for submission in subreddit.new(limit=2000):
    try:
        submission.comments.replace_more(limit=50)
        comments = [comment.body for comment in submission.comments.list()]

        post_list.append(
            {
                "title": submission.title,
                "score": submission.score,
                "url": submission.url,
                "author": submission.author,
                "created_utc": submission.created_utc,
                "num_comments": submission.num_comments,
                "id": submission.id,
                "upvote_ratio": submission.upvote_ratio,
                "selftext": submission.selftext,
                "comments": comments,
            }
        )
        idx += 1
        if (idx + 1) % 100 == 0:
            print(f"Collected {idx + 1} posts. Sleeping for 3 seconds...")
            time.sleep(3)
    except Exception:
        failed_posts.append(submission.id)
        continue

# # Save to CSV
import pandas as pd

df = pd.DataFrame(post_list)
df.to_csv("reddit_posts.csv", index=False, sep="|")

earliest_time = df["created_utc"].min()
import duckdb

duckdb.connect("reddit.db")
duckdb.sql(
    "CREATE TABLE reddit_posts AS SELECT * FROM read_csv_auto('reddit_posts.csv', sep='|')"
)
duckdb.sql(
    "CREATE TABLE reddit_comments AS SELECT * FROM read_csv_auto('reddit_comments.csv', sep='|')"
)
