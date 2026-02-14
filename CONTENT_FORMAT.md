# Content Format Specification
> **Version**: 2.0.0
> **Applies to**: `news_items.content` column in Supabase (PostgreSQL)

## Overview

The `content` field stores an **LLM-generated structured editorial summary** of the
original article — NOT the raw article text. This design choice:

- **Reduces reading time**: Section-based digest vs. wall-of-text originals
- **Improves SEO**: Clean, structured content with proper headings
- **Saves storage**: Summaries are 500–1500 chars vs. 5000–10000 raw chars
- **Consistent quality**: Every article follows the same editorial format

---

## 1. Content Format: Structured Markdown

Every `content` field follows this section template (LLM adapts as needed):

```markdown
## Overview
What is this about? Why should readers care? (2-3 sentences)

## Key Highlights
- Most important takeaway #1
- Key metric or benchmark result
- Notable quote or announcement
- Additional detail

## Technical Details
(For research/technical content — omitted for business news)
Architecture, methodology, key innovations, performance metrics

## Impact & Significance
What this means for the AI industry and practitioners.
```

### Content Characteristics

| Property | Value |
|----------|-------|
| Format | CommonMark Markdown |
| Language | English |
| Length | 500 – 1,500 characters |
| Sections | 2–4 `##` headings |
| Images | **None** (summaries are text-only) |
| Links | Minimal (original article URL in `original_url` column) |

---

## 2. Summary Field

The `summary` field stores a **2-3 sentence executive summary** for use on article
cards, list views, and meta descriptions.

```
OpenAI has released GPT-5, featuring major improvements in mathematical reasoning
and code generation. The model achieves 92.4% on the MATH benchmark, an 8.2%
improvement over GPT-4o.
```

| Property | Value |
|----------|-------|
| Format | Plain text (no Markdown) |
| Length | 100 – 300 characters |
| Purpose | Card preview, `<meta description>`, social sharing |

---

## 3. Editorial Screening

Every article passes through an **LLM editorial filter** before publication.
Articles are REJECTED if they are:

- User questions / community discussions (Ask HN, Reddit Q&A)
- Job postings, hiring threads
- Personal opinions without substance
- Vague rumors, unverified speculation
- Routine minor updates or changelogs
- Off-topic content (not AI/ML/tech)

Rejected articles are NOT stored in the database.

---

## 4. Database Schema Reference

```sql
-- Key columns in news_items
content       TEXT    NOT NULL  -- Structured editorial summary (Markdown sections)
summary       TEXT    NOT NULL  -- 2-3 sentence executive summary (plain text)
original_url  TEXT    UNIQUE    -- Link to original article
category      TEXT              -- AI | LLM | Hardware | Research | Industry
is_processed  BOOLEAN           -- true = LLM enriched, false = fallback content
```

### Invariants (enforced by pipeline)

1. **No NULL content**: Every row has a non-empty `content` value
2. **No NULL summary**: Every row has a non-empty `summary` value
3. **No "General" category**: Off-topic articles are rejected, not stored

---

## 5. Frontend Rendering Guide

### 5.1 Content Rendering (React + react-markdown)

```tsx
import ReactMarkdown from 'react-markdown';

function ArticleContent({ content }: { content: string }) {
  return (
    <article className="prose prose-lg max-w-none dark:prose-invert">
      <ReactMarkdown>{content}</ReactMarkdown>
    </article>
  );
}
```

### 5.2 Article Card (uses `summary`)

```tsx
function ArticleCard({ article }: { article: NewsItem }) {
  return (
    <div className="border rounded-lg p-4 hover:shadow-md transition">
      <h3 className="font-semibold text-lg">{article.title}</h3>
      <p className="text-gray-600 mt-2 text-sm">{article.summary}</p>
      <div className="mt-3 flex items-center gap-2 text-xs text-gray-400">
        <span>{article.source_name}</span>
        <span>·</span>
        <time>{article.published_at}</time>
        <span className="ml-auto px-2 py-0.5 bg-blue-100 text-blue-700 rounded">
          {article.category}
        </span>
      </div>
    </div>
  );
}
```

### 5.3 SEO Metadata

```tsx
export async function generateMetadata({ params }) {
  const article = await getArticle(params.id);
  return {
    title: article.title,
    description: article.summary,  // Uses the summary field
    openGraph: {
      title: article.title,
      description: article.summary,
      type: 'article',
      publishedTime: article.published_at,
    },
  };
}
```

---

## 6. Example Content

### Stored in `content` column:

```markdown
## Overview

OpenAI has launched GPT-5, the successor to GPT-4o, featuring substantial
improvements in mathematical reasoning, code generation, and multimodal
understanding. The model is available immediately for ChatGPT Plus subscribers.

## Key Highlights

- **MATH benchmark**: 92.4% accuracy (+8.2% over GPT-4o)
- **HumanEval**: 96.1% (+4.3%), setting a new state-of-the-art for code generation
- **MMLU**: 91.8% (+2.1%), demonstrating broad knowledge improvements
- Native multimodal support with improved image understanding
- 2x context window (256K tokens) compared to GPT-4o

## Technical Details

GPT-5 employs a refined Mixture-of-Experts (MoE) architecture with improved
routing efficiency. Training used a new curriculum learning approach that
prioritizes reasoning-heavy tasks in later training stages. The model also
introduces "chain-of-thought distillation" for faster inference.

## Impact & Significance

GPT-5 narrows the gap with specialized models on mathematical reasoning while
maintaining strong general-purpose capabilities. The expanded context window
and improved code generation make it particularly relevant for enterprise
development workflows and research applications.
```

### Stored in `summary` column:

```
OpenAI launched GPT-5 with major improvements in math reasoning (92.4% MATH
benchmark) and code generation (96.1% HumanEval). Available now for ChatGPT
Plus subscribers with a 256K context window.
```
