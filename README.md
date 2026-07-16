# Reddit comment scraper

Pulls full Reddit comment trees into SQLite. Python 3.12+, no dependencies.

## Why not Reddit's `.json` endpoints?

They no longer work unauthenticated. As of mid-2026 Reddit's edge returns
`403 Blocked` for `.json` on subreddits, posts, search, and users. It's a
path-level policy block, not a bot heuristic: the *same client* gets `200` on
Reddit's normal HTML pages, and a browser User-Agent on `.json` still gets 403.
Changing headers or IPs does not help.

This scrapes the [Arctic Shift](https://github.com/ArthurHeitmann/arctic_shift)
archive instead. Compared to Reddit's official OAuth API, for comment trees:

|                  | Arctic Shift            | Official OAuth API             |
| ---------------- | ----------------------- | ------------------------------ |
| Auth             | none                    | app registration + secrets     |
| Rate limit       | ~1000 / 32s             | 100 / min                      |
| Comment trees    | flat list + `parent_id` | recursive `morechildren` calls |
| Deleted comments | retained                | unavailable                    |
| Freshness        | ~hours behind           | live                           |

Arctic Shift returns a thread's comments as one flat list, so a full tree costs
1–2 requests and is rebuilt locally. The OAuth API instead makes you walk
"load more comments" stubs recursively.

Use the official API instead if you need live data (sub-hour freshness), or if
you need to *write* to Reddit.

## Usage

```bash
# One or more threads by post id
python reddit_scraper.py post 1hts599

# Every thread in a subreddit over a date window
python reddit_scraper.py subreddit learnmath --after 2025-03-01 --before 2025-03-04 --min-comments 10

# Print a stored thread as a nested tree
python reddit_scraper.py tree 1hts599
```

Data lands in `scripts/reddit.db` (gitignored). Crawls are resumable: finished
threads are recorded and skipped on rerun, so interrupt freely and rerun to
continue. Pass `--force` to refetch.

## Schema

- `posts` — id, subreddit, title, author, created_utc, score, num_comments, selftext, `raw` JSON
- `comments` — id, link_id, parent_id, author, body, created_utc, score, `raw` JSON
- `fetched_threads` — resume bookkeeping

Every record keeps the full original API response in `raw`, so fields not
promoted to columns are still queryable via SQLite's JSON functions.

Rebuild a tree in SQL-free Python:

```python
from reddit_scraper import db_connect, build_tree
roots = build_tree(db_connect(), "1hts599")   # nested dicts with .replies
```

## Two things worth knowing

**Pagination rewinds by one second.** The API sorts descending by default and
treats `after` as *exclusive*. Paginating naively drops any comment sharing the
boundary timestamp — real threads do have same-second comments. So this sorts
ascending, rewinds the cursor 1s per page, and dedupes on `id`.

**422s are retried.** The API intermittently returns `422` for queries that are
valid and succeed on retry, so 422 is treated as transient. A genuinely
malformed query still raises once retries are spent.

## Courtesy

Arctic Shift is free and volunteer-run. The script paces itself from the
`X-Ratelimit-*` headers it gets back and backs off on 429 — please leave that
in. Set a contact in `USER_AGENT` if you're running large crawls.
