# Content Format Specification
> **Version**: 2.1.0
> **Applies to**: `news_items.content` column in Supabase (PostgreSQL)

## Overview

The `content` field stores an **LLM-generated structured editorial summary** of the
original article — NOT the raw article text. Stored as an **HTML fragment** so the
frontend can inject it into the DOM and style it with Tailwind Typography (`prose`).

- **Reduces reading time**: Section-based digest vs. wall-of-text originals
- **Improves SEO**: Clean, structured content with proper headings
- **Saves storage**: Summaries are 500–1500 chars vs. 5000–10000 raw chars
- **Consistent quality**: Every article follows the same editorial format

**Pipeline**: LLM outputs **Markdown** (with `####` for section headings). The Python
crawler converts Markdown → HTML with the `markdown` library and stores the HTML
fragment in `content`. No `<html>`, `<body>`, or wrapper `<div>` — only content-level tags.

---

## 1. Content Format: HTML Fragment (Stored in DB)

The **database** stores **HTML**. The LLM produces **Markdown**; the crawler converts
it to HTML before insert. Section headings use `####` in Markdown so the converter
outputs `<h4>` (best fit for `prose-sm` on the frontend).

### Allowed HTML Tags (frontend renders only these)

| Tag | Purpose | Example |
|-----|---------|--------|
| `<p>` | Paragraph | `<p>This is a paragraph.</p>` |
| `<h4>` | Section heading | `<h4>Key Finding</h4>` |
| `<strong>` | Bold | `<strong>important</strong>` |
| `<em>` | Italic | `<em>Nature</em>` |
| `<ul>`, `<li>` | Unordered list | `<ul><li>Point one</li></ul>` |
| `<ol>`, `<li>` | Ordered list | `<ol><li>First</li></ol>` |
| `<a href="...">` | Link | `<a href="https://...">link text</a>` |
| `<blockquote>` | Quote | `<blockquote>Quote here</blockquote>` |
| `<code>` | Inline code | `<code>model.train()</code>` |

### Content Characteristics

| Property | Value |
|----------|-------|
| Stored format | HTML fragment (no wrapper tags) |
| Source | LLM Markdown → Python `markdown.markdown()` |
| Language | English |
| Length | 500 – 1,500 characters (pre-conversion) |
| Sections | 2–4 `####` in Markdown → `<h4>` in HTML |
| Images | **None** (summaries are text-only) |
| Links | Minimal (e.g. original article in `original_url`) |

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
content       TEXT    NOT NULL  -- Structured editorial summary (HTML fragment; see allowed tags above)
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

### 5.1 Content Rendering (HTML + Tailwind Typography)

`content` is stored as HTML. Inject it into the DOM and use Tailwind Typography
(`prose`) to style standard HTML tags. **Do not** use ReactMarkdown — the backend
already stores HTML.

```tsx
function ArticleContent({ content }: { content: string }) {
  return (
    <article
      className="prose prose-sm max-w-none dark:prose-invert"
      dangerouslySetInnerHTML={{ __html: content }}
    />
  );
}
```

Only the allowed tags (p, h4, strong, em, ul, ol, li, a, blockquote, code) are
produced by the crawler; prose styles them (paragraphs, headings, lists, links, etc.).

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

### Stored in `content` column (HTML fragment):

```html
<p>OpenAI has launched GPT-5, the successor to GPT-4o, featuring substantial
improvements in mathematical reasoning, code generation, and multimodal
understanding. The model is available immediately for ChatGPT Plus subscribers.</p>
<h4>Key Highlights</h4>
<ul>
<li><strong>MATH benchmark</strong>: 92.4% accuracy (+8.2% over GPT-4o)</li>
<li><strong>HumanEval</strong>: 96.1% (+4.3%), setting a new state-of-the-art for code generation</li>
<li><strong>MMLU</strong>: 91.8% (+2.1%), demonstrating broad knowledge improvements</li>
<li>Native multimodal support with improved image understanding</li>
<li>2x context window (256K tokens) compared to GPT-4o</li>
</ul>
<h4>Technical Details</h4>
<p>GPT-5 employs a refined Mixture-of-Experts (MoE) architecture with improved
routing efficiency. Training used a new curriculum learning approach that
prioritizes reasoning-heavy tasks in later training stages.</p>
<h4>Impact & Significance</h4>
<p>GPT-5 narrows the gap with specialized models on mathematical reasoning while
maintaining strong general-purpose capabilities.</p>
```

### Stored in `summary` column:

```
OpenAI launched GPT-5 with major improvements in math reasoning (92.4% MATH
benchmark) and code generation (96.1% HumanEval). Available now for ChatGPT
Plus subscribers with a 256K context window.
```
