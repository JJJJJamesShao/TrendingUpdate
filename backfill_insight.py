import os
import json
import time
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
from openai import OpenAI

# 加载环境变量
load_dotenv()

# 初始化 AI 客户端
client = OpenAI(
    api_key=os.getenv("QWEN_API_KEY"),
    base_url=os.getenv("QWEN_BASE_URL"),
)
MODEL_NAME = os.getenv("QWEN_MODEL")

# 数据库连接参数 (根据你的 .env 更新了键名)
DB_PARAMS = {
    "host": os.getenv("host"),
    "port": os.getenv("port"),
    "dbname": os.getenv("dbname"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
}

# 每次从数据库拉取的批次大小
BATCH_SIZE = 10 
# API 请求失败时的重试次数
MAX_RETRIES = 3

def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)

def generate_insight_and_tags(title, content, summary):
    """调用大模型生成 tag 和 insight"""
    
    text_to_analyze = content if content else summary
    if not text_to_analyze:
        text_to_analyze = title
        
    system_prompt = """You are a senior AI industry editor. 
Your task is to analyze an AI news article and output ONLY a valid JSON object.
Do not wrap the JSON in markdown code blocks (e.g., ```json). Just output the raw JSON.
"""

    user_prompt = f"""
Analyze the following AI article:
Title: {title}
Content: {text_to_analyze[:3000]}

Provide:
1. "tags": Select 1 to 3 tags from this exact list: ["LLM", "Agents", "Infra", "Tools", "Research", "Industry", "Business"].
2. "insight": A highly opinionated, sharp 1-sentence takeaway (under 20 words in English) explaining why this matters to developers or AI businesses.

JSON Format:
{{
  "tags": ["tag1", "tag2"],
  "insight": "Your sharp 1-sentence takeaway here."
}}
"""

    for attempt in range(MAX_RETRIES):
        try:
            response = client.chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.3,
            )
            
            raw_result = response.choices[0].message.content.strip()
            result_dict = json.loads(raw_result)
            
            tags = result_dict.get("tags", [])
            insight = result_dict.get("insight", "")
            
            if not isinstance(tags, list):
                tags = []
                
            return tags, insight
            
        except Exception as e:
            print(f"[Warning] AI 调用失败 (尝试 {attempt + 1}/{MAX_RETRIES}): {e}")
            time.sleep(2)
            
    return [], ""

def run_backfill():
    total_processed = 0
    
    while True:
        # --- 阶段 1：获取数据 ---
        # 建立连接，拉取完数据后立刻关闭，不占用连接池
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT id, title, content, summary 
                FROM news_items 
                WHERE tags IS NULL OR insight IS NULL
                LIMIT %s
            """, (BATCH_SIZE,))
            rows = cur.fetchall()
        except Exception as e:
            print(f"❌ 读取数据库数据时出错: {e}")
            break
        finally:
            cur.close()
            conn.close() # 立刻释放连接
            
        if not rows:
            print("🎉 所有存量数据处理完毕！")
            break
            
        print(f"📦 正在处理新批次，本批次 {len(rows)} 条数据...")
        
        # --- 阶段 2：离线处理（不再占用数据库连接） ---
        updates = []
        for row in rows:
            article_id, title, content, summary = row
            
            print(f"  -> 提纯中: {title[:40]}...")
            tags, insight = generate_insight_and_tags(title, content, summary)
            
            if tags or insight:
                updates.append((tags, insight, article_id))
                
            time.sleep(1) # 控制 API 频率
        
        # --- 阶段 3：写入数据 ---
        if updates:
            # AI 处理完毕，重新建立连接写入数据库
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                update_query = """
                    UPDATE news_items 
                    SET tags = %s, insight = %s 
                    WHERE id = %s
                """
                execute_batch(cur, update_query, updates)
                conn.commit()
                total_processed += len(updates)
                print(f"✅ 成功将 {len(updates)} 条更新写入数据库。当前总进度: {total_processed} 条。")
            except Exception as e:
                print(f"❌ 写入数据库时出错: {e}")
                conn.rollback()
            finally:
                cur.close()
                conn.close() # 写入完立刻释放

if __name__ == "__main__":
    print("🚀 开始执行历史数据 Backfill 任务...")
    run_backfill()