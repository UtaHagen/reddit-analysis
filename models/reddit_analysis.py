import duckdb
from duckdb.typing import VARCHAR
from pydantic import BaseModel
from ollama import chat
import pandas as pd
import json

conn = duckdb.connect(
    "/Users/alexvalentine/Desktop/Project/data-science-template/data/raw/reddit.db"
)

conn.sql("""
    CREATE TABLE IF NOT EXISTS reddit_posts AS 
    SELECT * 
    FROM read_csv_auto('/Users/alexvalentine/Desktop/Project/data-science-template/data/raw/reddit_posts.csv', sep='|')
""")

# emotion_labels = ["sadness", "joy", "frustration", "anger", "fear", "surprise", "anxiety", "other", "lost", "neutral", "worried"]
# topics = ["work", "exam", "advise", "other"]
product_analysis_prompt = """
You are a product manager for designing an actuarial exam education product. 

Analyze the reddit posts and comments to understand the emotions, situations, actions and user intent.

Return the output in JSON format:

- emotion: string - the dominant emotion reflected in the post and comments (e.g. frustration, anxiety, hope)
- situation: string - a concise summary of the situation the user is experiencing. Focus on what caused the emotion; the post text is the main indicator.
- action: string - what the user is likely to do or consider doing in response. Use the comments as clues to what actions are encouraged or planned.
- intent: string - the underlying human intent or psychological drive behind the post or comment (e.g. looking for recognition, seeking validation, expressing frustration). Avoid restating the text; infer the psychological motivation.

Return only valid JSON with no additional text or formatting.
title: {post_title}
post: {post}
comments: {comments}
"""

persona_prompt = """
You are a product manager for designing an actuarial exam education product. 

Analyze the reddit posts and comments to understand the user persona and study scenario.

Return the output in JSON format:

- persona: string - the likely user persona (e.g. formula-focused candidate, repeat test taker, first-time candidate, student)
- scenario: string - the most relevant study scenario for this post (e.g. after work, before bed, weekend, final month before exam)
- need_statement: string - the needs of the user (e.g. formula memorization, practice efficiency, resource filtering, stress management, community support, study planning, exam simulation, post-exam review, information transparency, accountability system)

Return only valid JSON with no additional text or formatting.
title: {post_title}
post: {post}
comments: {comments}
"""

solution_prompt = """
You are helping me analyze user needs from Reddit posts related to actuarial exam study. For each need statement, you will help me determine how strong the “alternative solution” is, based on user comments.

Instructions:
For each need statement, review the comments. Judge how easily users can find or create their own solutions.

Use this scale:
-  1: Everyone can easily get it; there is no barrier to access.
-  2: Resources exist but are scattered or require effort to find or organize.
-  3: There are DIY solutions; many people try them but often complain they don’t really work.
-  4: Only a few people have found a workaround, but most can’t replicate it.
-  5: No one has a real solution; people are stuck and frustrated.

Return the output in JSON format:
- solution: string - the alternative solution to the need statement
- solution_score: int - the score of the alternative solution (1-5)
- confidence_score: float - the confidence score of the alternative solution (0-1)

Return only valid JSON with no additional text or formatting.

title: {post_title}
need_statement: {need_statement}
comments: {comments}
"""


# Define the schema for the response
class ProductAnalysis(BaseModel):
    emotion: str
    situation: str
    action: str
    intent: str


class PersonaAnalysis(BaseModel):
    persona: str
    scenario: str
    need_statement: str


class SolutionAnalysis(BaseModel):
    solution: str
    solution_score: int
    confidence_score: float


class SolutionScore(BaseModel):
    solution_score: int
    confidence_score: float


def text_analysis(post_title, post, comments):
    prompt = product_analysis_prompt.format(
        post_title=post_title, post=post, comments=comments
    )
    response = chat(
        model="llama3:latest",
        messages=[{"role": "user", "content": prompt}],
        format=ProductAnalysis.model_json_schema(),  # Use Pydantic to generate the schema or format=schema
        options={"temperature": 0},  # Make responses more deterministic
    )
    product_analysis_response = ProductAnalysis.model_validate_json(
        response.message.content
    )
    return product_analysis_response.model_dump_json()


def persona_analysis(post_title, post, comments):
    prompt = persona_prompt.format(post_title=post_title, post=post, comments=comments)
    response = chat(
        model="llama3:latest",
        messages=[{"role": "user", "content": prompt}],
        format=PersonaAnalysis.model_json_schema(),  # Use Pydantic to generate the schema or format=schema
        options={"temperature": 0},  # Make responses more deterministic
    )
    persona_analysis_response = PersonaAnalysis.model_validate_json(
        response.message.content
    )
    return persona_analysis_response.model_dump_json()


def solution_analysis(post_title, need_statement, comments):
    prompt = solution_prompt.format(post_title=post_title, need_statement=need_statement, comments=comments)
    response = chat(
        model="llama3.1:latest",
        messages=[{"role": "user", "content": prompt}],
        format=SolutionAnalysis.model_json_schema(),
        options={"temperature": 0},
    )
    solution_analysis_response = SolutionAnalysis.model_validate_json(
        response.message.content
    )
    return solution_analysis_response.model_dump_json()


conn.create_function(
    "text_analysis", text_analysis, [VARCHAR, VARCHAR, VARCHAR], VARCHAR
)
conn.create_function(
    "persona_analysis", persona_analysis, [VARCHAR, VARCHAR, VARCHAR], VARCHAR
)
conn.create_function(
    "solution_analysis", solution_analysis, [VARCHAR, VARCHAR, VARCHAR], VARCHAR
)

res = conn.sql(
    "SELECT text_analysis(title, selftext, comments) FROM reddit_posts"
).fetchall()
print(res)
conn.sql(
    """
    CREATE TABLE reddit_persona_analysis AS
    SELECT id, persona_analysis(title, selftext, comments) AS persona_analysis FROM reddit_posts
    """
)
conn.sql(
    """
    CREATE TABLE reddit_solution_analysis AS
    SELECT reddit_posts.id AS is, solution_analysis(reddit_posts.title, silver.reddit_llm.need_statement, reddit_posts.comments) AS solution_analysis 
    FROM reddit_posts
    LEFT JOIN silver.reddit_llm 
    ON reddit_posts.id = silver.reddit_llm.id
    """
)
df = pd.DataFrame(res)

df_source = conn.sql("SELECT * FROM reddit_posts").df()
df_merge = df_source.join(df)

# 将列0中的JSON对象展开为4个独立的列
import json

df_merge = df_merge.join(
    pd.json_normalize(df_merge[0].fillna("Unknown").apply(json.loads))
)
df_merge = df_merge.drop(columns=[0])

conn.sql("""
         CREATE TABLE reddit_analysis AS
         SELECT * FROM df_merge
         """)


conn.sql("""
         CREATE TABLE llm_analysis AS
         WITH data AS (
         SELECT pa.*, ra."0" AS user_analysis
         FROM reddit_analysis ra
         JOIN reddit_persona_analysis pa ON ra.id = pa.id)
         SELECT * FROM data
         """)

conn.sql("""
         CREATE TABLE silver.reddit_llm AS
WITH data AS(
SELECT id,
  "persona_analysis(title, selftext, ""comments"")"::JSON::STRUCT(
  persona VARCHAR, scenario VARCHAR, need_statement VARCHAR
  ) AS persona_analysis,
  user_analysis::JSON::STRUCT(emotion VARCHAR, situation varchar, action VARCHAR, intent varchar) AS user_analysis 
FROM llm_analysis)
SELECT 
  id,
  persona_analysis.persona,
  persona_analysis.scenario,
  persona_analysis.need_statement,
  user_analysis.emotion,
  user_analysis.situation,
  user_analysis.action,
  user_analysis.intent
FROM data
""")

# analyze the needs
conn.sql("""
         WITH temp AS (
        SELECT DISTINCT need_statement FROM silver.reddit_llm
        WHERE situation ilike '%exam%'),
        needs AS (
        SELECT unnest(split(need_statement, ', ')) AS need_statement FROM temp)
        SELECT  need_statement, count(*) AS count
        FROM needs
        GROUP BY need_statement
        ORDER BY count DESC
         """)


conn.close()
