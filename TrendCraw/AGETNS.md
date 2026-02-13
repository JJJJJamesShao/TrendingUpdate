Project Name: AI Nexus - High-Frequency Global AI Intelligence Aggregator
Version: 2.0.0 (Vibe Coding Edition)
Goal: Build a fully automated, high-frequency AI news aggregation engine using Python and GitHub Actions.
Constraint Checklist:

[x] Zero-Cost Infrastructure: GitHub Actions (Runner) + Supabase (Free Tier).

[x] Agile Deployment: Pure Python implementation (No heavy Docker/Selenium/Headless Browser).

[x] Maximized Sources: Extensive RSS/URL coverage.

[x] Smart De-duplication: LLM-based content clustering + Authority bias.

[x] High Frequency: Optimized async execution for sub-hourly updates.

1. System Architecture
1.1 The Pipeline (Python Script: etl_core.py)
This script runs in a single GitHub Action workflow. It is stateless but uses the Database for state.

代码段
graph TD
    A[Scheduler (GitHub Actions)] -->|Every 30-60 mins| B(ETL Runner)
    B --> C{Source Fetcher}
    C -->|Parallel Async| D[RSS Feeds & Web Scrapers]
    D --> E[Raw Content Pool]
    E --> F{De-duplication Engine}
    F -->|Step 1: Exact URL Match| G[Filter Known]
    F -->|Step 2: Semantic Clustering (LLM)| H[Group Similar Stories]
    H --> I[Selection Logic (Earliest/Best Source)]
    I --> J[Content Processor (LLM)]
    J -->|Summarize & Tag| K[(Supabase DB)]
1.2 Technology Stack
Language: Python 3.10+

Database: Supabase (PostgreSQL + pgvector optional but recommended for future).

Network/Fetching: aiohttp (Async requests), feedparser (RSS).

Parsing/Rendering: Jina Reader API (https://r.jina.ai/) - Crucial for avoiding headless browsers.

Intelligence: OpenAI/Anthropic API (Your Paid Agent) for deduplication and summarization.

Similarity Check: rapidfuzz (Local fast string matching) + LLM.

2. Data Sources (The "firehose")
Instruction to Agent: Use this EXACT list. Do not sample. Fetch ALL of them.

Tier 1: Corporate Labs & Official (High Authority)
OpenAI Blog: https://openai.com/index.xml

Anthropic Research: https://www.anthropic.com/index.xml

Google DeepMind: https://deepmind.google/blog/rss.xml

Microsoft Research: https://www.microsoft.com/en-us/research/feed/

Meta AI (Facebook): https://ai.meta.com/blog/rss.xml

Hugging Face Blog: https://huggingface.co/blog/feed.xml

NVIDIA Blog (AI Category): https://blogs.nvidia.com/blog/category/deep-learning/feed/

AWS Machine Learning: https://aws.amazon.com/blogs/machine-learning/feed/

Tier 2: Tech News & Media (High Volume)
TechCrunch (AI): https://techcrunch.com/category/artificial-intelligence/feed/

The Verge (AI): https://www.theverge.com/rss/artificial-intelligence/index.xml

VentureBeat (AI): https://venturebeat.com/category/ai/feed/

MIT Technology Review: https://www.technologyreview.com/topic/artificial-intelligence/feed

Ars Technica (AI): https://arstechnica.com/tag/ai/feed/

Wired (AI): https://www.wired.com/feed/tag/ai/latest/rss

Tier 3: Developer & Community (High Technicality)
Hacker News (Algorithm filter): Fetch via API, filter for 'GPT', 'LLM', 'Transformer'

Reddit r/LocalLLaMA: https://www.reddit.com/r/LocalLLaMA/new/.json (User-Agent required)

Reddit r/MachineLearning: https://www.reddit.com/r/MachineLearning/new/.json

Simon Willison's Weblog: https://simonwillison.net/atom/entries/

Lil'Log (Lilian Weng): https://lilianweng.github.io/index.xml

Last Week in AI: https://lastweekin.ai/feed

Tier 4: Paper & Research
ArXiv (CS.AI): Use arxiv python lib, fetch last 24h

ArXiv (CS.CL): Use arxiv python lib, fetch last 24h

Hugging Face Daily Papers: https://huggingface.co/papers/feed

3. Workflow Logic & Business Rules
Step 1: The "Jina" Fetch Strategy
To avoid deploying Chrome/Selenium in GitHub Actions (which is slow and heavy):

For RSS: Use feedparser.

For Web Links: Construct URL as https://r.jina.ai/<TARGET_URL>.

Why: Jina returns clean Markdown. It handles the JS rendering on their side.

Header: Use Authorization: Bearer <JINA_KEY> (Free tier is generous, but key is recommended).

Step 2: Intelligent Deduplication (The "Cluster" Logic)
We will likely fetch the same news from TechCrunch, Verge, and OpenAI.
Algorithm:

Time Window: Only load data from the last 24 hours.

Normalization: Strip punctuation, lowercase titles.

Local Filter: Use rapidfuzz to find titles with >85% similarity to existing DB entries. If found -> SKIP.

LLM Clustering (The "Smart" Layer):

Input: A batch of 20-30 newly fetched titles + summaries.

Prompt: "Group these items by event. If 3 articles talk about 'GPT-5 Release', group them. Select the Primary Source (Tier 1 is best, Tier 2 second). If no Tier 1, select the Earliest timestamp."

Output: A list of unique events to be inserted.

Step 3: Content Enrichment
For each unique event selected:

Call Your Paid Agent (LLM).

Prompt: "Translate summary to Chinese. Extract 5 technical tags. Rate importance (1-10). sentiment analysis."

4. Scheduling & Deployment Strategy (GitHub Actions)
4.1 Frequency Optimization
To serve NA/EU users while keeping costs low:

Peak Hours (08:00 - 20:00 EST/UTC-5): Run every 30 minutes.

Off-Peak: Run every 2 hours.

APAC Consideration: The "Peak Hours" cover late night APAC, so updates are ready for them in the morning.

4.2 The Workflow File (.github/workflows/news-pipe.yml)
Key Configuration:

Concurrency: group: news-scraper cancel-in-progress: true (Prevents overlap).

Timeout: timeout-minutes: 10 (If it hangs, kill it to save minutes).

Matrix: None. Single job.

4.3 Environment Variables (Secrets)
SUPABASE_URL: DB Connection.

SUPABASE_KEY: Service Role Key (Required for writing).

LLM_API_KEY: For your paid agent.

JINA_API_KEY: (Optional but recommended) For scraping.

5. Coding Instructions for the Agent
When generating code, follow these rules:

Async First: Use asyncio and aiohttp. We are fetching 20+ sources; synchronous code is unacceptable.

Error Handling: If one RSS feed fails, log it and continue. Do not crash the whole script.

Database Idempotency: Use ON CONFLICT (url) DO NOTHING or similar logic.

Log Verbosity: Print clear logs ("Fetched 50 items", "Deduplicated 10 items", "Inserted 5 items") so we can debug via GitHub Actions logs.

Modular: Separate fetchers.py, processors.py, and main.py.