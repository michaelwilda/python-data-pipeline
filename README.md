# 🔬 Python Data Pipeline — Hacker News Analytics

> A production-quality data pipeline that **scrapes, cleans, transforms, and visualizes**
> Hacker News story data — demonstrating professional Python engineering practices end-to-end.

![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python&logoColor=white)
![CI](https://github.com/michaelwilda/python-data-pipeline/actions/workflows/ci.yml/badge.svg)
![Coverage](https://img.shields.io/badge/coverage-80%25%2B-brightgreen)
![License](https://img.shields.io/badge/License-MIT-green)

---

## ✨ Highlights

- **Scraping** — `requests` + `BeautifulSoup4` with automatic retries, timeouts, and polite rate-limiting
- **Pandas pipeline** — full `clean → transform → aggregate` chain using the `.pipe()` pattern
- **Three charts** — `matplotlib` figures saved to PNG, ready to embed anywhere
- **Strict typing** — `from __future__ import annotations` + full type hints on every signature
- **Google-style docstrings** — every class and function documented
- **80 %+ test coverage** — `pytest` with `unittest.mock`; zero live HTTP calls in tests
- **GitHub Actions CI** — lint (`flake8`) + test matrix across Python 3.10 / 3.11 / 3.12

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                        HN DATA PIPELINE                              │
│                                                                      │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐           │
│  │  1. COLLECT  │───▶│  2. CLEAN    │───▶│ 3. TRANSFORM │           │
│  │              │    │              │    │              │           │
│  │ scraper/     │    │ pipeline/    │    │ pipeline/    │           │
│  │ scraper.py   │    │ cleaner.py   │    │ transformer  │           │
│  │              │    │              │    │ .py          │           │
│  │ ▼ raw.csv    │    │ ▼ clean.csv  │    │ ▼ transf.csv │           │
│  └──────────────┘    └──────────────┘    └──────┬───────┘           │
│                                                  │                   │
│                                          ┌───────▼───────┐          │
│                                          │ 4. VISUALIZE  │          │
│                                          │               │          │
│                                          │ visualizer/   │          │
│                                          │ charts.py     │          │
│                                          │               │          │
│                                          │ ▼ *.png       │          │
│                                          └───────────────┘          │
│                                                                      │
│             All stages orchestrated by  main.py  (argparse CLI)     │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 📋 Prerequisites

- Python **3.10 or later**
- `pip`

---

## 🚀 Quick Start

```bash
git clone https://github.com/michaelwilda/python-data-pipeline.git
cd python-data-pipeline
pip install -r requirements.txt
```

### Run the full pipeline

```bash
python main.py --all
```

This executes all four stages sequentially and writes every output file to
`data/output/`.

---

## 💻 CLI Reference

| Flag | Description |
|------|-------------|
| `--all` | Run every stage: scrape → clean → transform → visualize |
| `--scrape` | Fetch raw stories from HN and write `raw.csv` |
| `--clean` | Normalise `raw.csv` → `clean.csv` |
| `--transform` | Enrich `clean.csv` → `transformed.csv` |
| `--visualize` | Generate charts from `transformed.csv` |
| `--pages N` | Number of HN pages to scrape (default: 3, 30 stories/page) |
| `--verbose` / `-v` | Enable DEBUG-level logging |

### Examples

```bash
# Scrape 5 pages (~150 stories)
python main.py --scrape --pages 5

# Clean previously scraped data
python main.py --clean

# Transform and show summary stats
python main.py --transform

# Generate all three charts
python main.py --visualize

# Full pipeline, verbose logging
python main.py --all --verbose
```

---

## 📁 Repository Structure

```
python-data-pipeline/
├── config.py                   # ALL constants and tuneable settings
├── main.py                     # CLI entry point (argparse)
│
├── scraper/
│   ├── __init__.py
│   └── scraper.py              # HNScraper: requests + BeautifulSoup4
│
├── pipeline/
│   ├── __init__.py
│   ├── cleaner.py              # DataCleaner: normalise & validate
│   └── transformer.py          # DataTransformer: enrich & aggregate
│
├── visualizer/
│   ├── __init__.py
│   └── charts.py               # ChartGenerator: matplotlib figures
│
├── data/
│   └── output/                 # Generated CSVs and PNGs (git-ignored)
│
├── tests/
│   ├── conftest.py
│   ├── test_scraper.py
│   ├── test_pipeline.py
│   ├── test_transformer.py
│   ├── test_visualizer.py
│   └── test_main.py
│
├── .github/workflows/ci.yml    # GitHub Actions — lint + test matrix
├── .flake8                     # Linting configuration
├── .gitignore
├── pyproject.toml
├── requirements.txt
└── README.md
```

---

## 📊 Output Files

After running `--all`, the following files appear in `data/output/`:

| File | Description |
|------|-------------|
| `raw.csv` | Raw scraped data (90 rows for default 3 pages) |
| `clean.csv` | Deduplicated, type-coerced, filtered data |
| `transformed.csv` | Enriched data with 5 derived columns |
| `top_domains.png` | Horizontal bar chart — top-10 domains by story count |
| `score_distribution.png` | Histogram — score distribution with mean & median lines |
| `score_vs_comments.png` | Scatter plot — score vs. comments, coloured by engagement tier |

### Derived Columns (added during transform)

| Column | Description |
|--------|-------------|
| `domain_clean` | Lower-cased domain with `www.` stripped |
| `score_per_comment` | Score ÷ comments (0 when no comments) |
| `engagement_tier` | `High` / `Medium` / `Low` based on score percentiles |
| `score_rank` | Global rank by score (1 = highest) |
| `domain_story_count` | Number of stories from the same domain in this dataset |

---

## 📈 Charts Explained

### 1 — Top Domains Bar Chart
Horizontal bars showing which domains appear most frequently in the
current dataset. Value labels make precise counts easy to read at a glance.

### 2 — Score Distribution Histogram
Frequency distribution of story scores. Dashed red line = mean; dotted
grey line = median. The typically right-skewed shape reflects HN's
power-law engagement pattern.

### 3 — Score vs. Comments Scatter
Each point is one story. Red = High engagement tier, amber = Medium,
grey = Low. Clusters reveal whether high-scoring stories also attract
high comment counts (they often do not — readers sometimes upvote
without commenting).

---

## 🧪 Tests

```bash
# Run all tests with coverage report
pytest tests/ -v

# Run a single module
pytest tests/test_scraper.py -v

# Coverage only (no verbose output)
pytest tests/ --cov=. --cov-report=term-missing
```

The test suite mocks all HTTP calls — no network access required.
Coverage is enforced at **≥ 80 %** via `pyproject.toml`.

---

## ⚖️ Ethical Scraping

This project follows responsible scraping practices:

| Practice | Implementation |
|----------|----------------|
| `robots.txt` compliance | HN disallows only `/x?fnid=*`; `/news` is unrestricted |
| Rate limiting | 2-second delay between every page request (`RATE_LIMIT_DELAY`) |
| Transparent `User-Agent` | Identifies the pipeline and links to this repository |
| Public data only | No authentication, no private endpoints |
| Minimal server load | Default scrape = 3 pages = 3 HTTP requests |

See `config.py` to adjust `RATE_LIMIT_DELAY` if needed.

---

## 🛠️ Tech Stack

| Library | Purpose |
|---------|---------|
| `requests` | HTTP client with retry/back-off via `urllib3` |
| `beautifulsoup4` | HTML parsing with `lxml` back-end |
| `pandas` | DataFrame operations throughout the pipeline |
| `matplotlib` | Chart generation (Agg backend — headless-safe) |
| `pytest` + `pytest-cov` | Unit testing and coverage enforcement |
| `flake8` + `flake8-bugbear` | Linting and anti-pattern detection |
| GitHub Actions | CI matrix: Python 3.10, 3.11, 3.12 |

---

## 📝 License

MIT © Michael Wilda
