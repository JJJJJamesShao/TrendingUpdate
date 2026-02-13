# Content Format Specification
> **Version**: 1.0.0
> **Applies to**: `news_items.content` column in Supabase (PostgreSQL)

## Overview

The `content` field stores the **full article text** extracted from the original source,
formatted as **Markdown** with embedded image references. This document defines the
format contract between the backend crawler (TrendCraw) and the frontend renderer.

---

## 1. Text Format: Markdown

All content is stored as standard **CommonMark Markdown**. The frontend should use a
Markdown renderer (e.g., `react-markdown`, `marked`, `markdown-it`) to display it.

### Supported Elements

| Element | Markdown Syntax | Example |
|---------|----------------|---------|
| Heading | `## Title` | `## Key Findings` |
| Paragraph | Plain text block | `OpenAI announced a new model today...` |
| Bold | `**text**` | `**important**` |
| Italic | `*text*` | `*emphasis*` |
| Link | `[text](url)` | `[official blog](https://openai.com/blog)` |
| **Image** | `![alt](url)` | `![GPT-4o architecture](https://cdn.openai.com/img.jpg)` |
| Bullet list | `- item` | `- Improved reasoning` |
| Numbered list | `1. item` | `1. First step` |
| Code | `` `code` `` | `` `transformer` `` |
| Code block | ` ```lang ... ``` ` | Fenced code blocks |
| Blockquote | `> text` | `> According to the paper...` |

---

## 2. Image Format

### 2.1 Storage Format

Images are stored as **standard Markdown image references** pointing to the **original
source URL** (hotlinked):

```markdown
![alt text](https://example.com/path/to/image.jpg)
```

#### Components

| Part | Description | Example |
|------|-------------|---------|
| `alt text` | Image description (from HTML `alt` attribute) | `GPT-4o benchmark results` |
| `url` | **Absolute** URL to the original image on the source website | `https://cdn.openai.com/research/img/benchmark.png` |

### 2.2 URL Characteristics

- All image URLs are **absolute** (protocol + domain + path)
- Relative URLs are resolved during extraction using the article's source URL
- Common image CDN domains: `cdn.openai.com`, `wp-content/uploads/...`, `cdn.arstechnica.net`, etc.
- Formats: `.jpg`, `.png`, `.webp`, `.gif`, `.svg`

### 2.3 Edge Cases

| Case | Behavior |
|------|----------|
| Image has no `alt` text | Stored as `![](url)` (empty alt) |
| Image uses `data-src` (lazy load) | May not be captured (limitation of static HTML parsing) |
| Image is a Base64 data URI | Stripped during extraction (too large for storage) |
| Image behind authentication | URL preserved but may return 403 when rendered |
| SVG inline | Stripped during HTML cleaning (not stored) |

---

## 3. Content Extraction Pipeline

Content is extracted through a multi-strategy pipeline. Each strategy has different
image preservation capabilities:

| Priority | Strategy | Images Preserved | Format |
|----------|----------|-----------------|--------|
| S1 | Heuristic HTML isolation + html2text | **Yes** `![alt](url)` | Markdown |
| S2 | trafilatura (smart extraction) | No (plain text) | Text |
| S3 | html2text on full cleaned page | **Yes** `![alt](url)` | Markdown |
| S4 | LLM extraction (MiniMax) | **Yes** (prompt-specified) | Markdown |
| Fallback | RSS snippet | No | Plain text |

The pipeline uses **image-aware comparison**: when S1 finds images, it is strongly
preferred over S2 even if S2 has more text content.

---

## 4. Frontend Rendering Guide

### 4.1 Basic Rendering (React + react-markdown)

```tsx
import ReactMarkdown from 'react-markdown';

function ArticleContent({ content }: { content: string }) {
  return (
    <article className="prose prose-lg max-w-none">
      <ReactMarkdown
        components={{
          img: ({ src, alt }) => (
            <ArticleImage src={src || ''} alt={alt || ''} />
          ),
        }}
      >
        {content}
      </ReactMarkdown>
    </article>
  );
}
```

### 4.2 Image Component with Error Handling (Plan D)

```tsx
'use client';

import { useState } from 'react';
import Image from 'next/image';

interface ArticleImageProps {
  src: string;
  alt: string;
}

export function ArticleImage({ src, alt }: ArticleImageProps) {
  const [error, setError] = useState(false);
  const [loading, setLoading] = useState(true);

  if (error) {
    return (
      <div className="my-4 flex items-center justify-center rounded-lg
                      bg-gray-100 dark:bg-gray-800 p-8 text-sm text-gray-500">
        <span>{alt || 'Image unavailable'}</span>
      </div>
    );
  }

  return (
    <figure className="my-6">
      {loading && (
        <div className="animate-pulse rounded-lg bg-gray-200 dark:bg-gray-700 h-64 w-full" />
      )}
      <img
        src={src}
        alt={alt}
        loading="lazy"
        onLoad={() => setLoading(false)}
        onError={() => { setError(true); setLoading(false); }}
        className={`rounded-lg w-full ${loading ? 'hidden' : 'block'}`}
      />
      {alt && !loading && (
        <figcaption className="mt-2 text-center text-sm text-gray-500">
          {alt}
        </figcaption>
      )}
    </figure>
  );
}
```

### 4.3 Optional: Image Proxy for Production

For production deployments, consider routing images through a free proxy to avoid
hotlinking issues and enable optimization:

```tsx
function proxyImageUrl(originalUrl: string): string {
  // wsrv.nl — free, open-source image proxy with CDN
  return `https://wsrv.nl/?url=${encodeURIComponent(originalUrl)}&w=800&output=webp`;
}

// Usage in ArticleImage component:
<img src={proxyImageUrl(src)} alt={alt} loading="lazy" ... />
```

Benefits of proxy approach:
- Bypasses `Referer` restrictions on hotlinked images
- Automatic WebP conversion (smaller file size)
- Resize on the fly (`&w=800`)
- CDN caching (faster load times)
- No storage cost

---

## 5. Database Schema Reference

```sql
-- Relevant columns in news_items table
content     TEXT        -- Markdown article body (may include ![alt](url) images)
summary     TEXT        -- LLM-generated summary (plain text, no images)
original_url TEXT       -- Link to original article on source website
```

### Content Size

| Metric | Value |
|--------|-------|
| Max stored length | 10,000 characters |
| Typical article | 3,000 – 8,000 characters |
| Images per article | 0 – 10 (typically 1-3) |

---

## 6. Example Content

Below is a representative example of stored content:

```markdown
## OpenAI Launches GPT-5 with Enhanced Reasoning

![GPT-5 announcement banner](https://cdn.openai.com/assets/gpt5-hero.jpg)

OpenAI today announced GPT-5, the latest iteration of its flagship language model,
featuring significant improvements in mathematical reasoning and code generation.

The new model achieves **state-of-the-art performance** on multiple benchmarks:

- MATH benchmark: 92.4% (+8.2% over GPT-4o)
- HumanEval: 96.1% (+4.3% over GPT-4o)
- MMLU: 91.8% (+2.1% over GPT-4o)

![Benchmark comparison chart](https://cdn.openai.com/research/gpt5-benchmarks.png)

### Availability

GPT-5 is available today for [ChatGPT Plus](https://chat.openai.com) subscribers
and will roll out to API users over the next two weeks.

> "This represents our most capable model to date," said Sam Altman,
> CEO of OpenAI, during the announcement.
```
