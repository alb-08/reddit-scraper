#!/usr/bin/env python3
"""Scrape Reddit comment trees via the Arctic Shift archive API.

Reddit's unauthenticated .json endpoints return 403 as of mid-2026, so this
pulls from Arctic Shift instead. It returns each thread's comments as a flat
list carrying parent_id, so trees are rebuilt locally rather than by walking
Reddit's "more comments" stubs.

Usage:
    python reddit_scraper.py post <post_id> [post_id ...]
    python reddit_scraper.py subreddit <name> --after 2025-01-01 --before 2025-02-01
    python reddit_scraper.py tree <post_id>
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Iterator

API = "https://arctic-shift.photon-reddit.com/api"
USER_AGENT = "reddit-comment-scraper/1.0 (github.com/edexcel-maths-anki)"
PAGE_LIMIT = 100
DB_PATH = Path(__file__).parent / "reddit.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS posts (
    id            TEXT PRIMARY KEY,
    subreddit     TEXT,
    title         TEXT,
    author        TEXT,
    created_utc   INTEGER,
    score         INTEGER,
    num_comments  INTEGER,
    permalink     TEXT,
    selftext      TEXT,
    raw           TEXT
);
CREATE TABLE IF NOT EXISTS comments (
    id            TEXT PRIMARY KEY,
    link_id       TEXT NOT NULL,
    parent_id     TEXT NOT NULL,
    subreddit     TEXT,
    author        TEXT,
    body          TEXT,
    created_utc   INTEGER,
    score         INTEGER,
    permalink     TEXT,
    raw           TEXT
);
CREATE INDEX IF NOT EXISTS idx_comments_link   ON comments(link_id);
CREATE INDEX IF NOT EXISTS idx_comments_parent ON comments(parent_id);
CREATE TABLE IF NOT EXISTS fetched_threads (
    link_id       TEXT PRIMARY KEY,
    comment_count INTEGER,
    fetched_at    INTEGER
);
"""


class RateLimiter:
    """Paces requests using the API's own X-Ratelimit-* headers.

    The server reports remaining quota per window; when it runs low we wait out
    the window rather than guessing a fixed delay.
    """

    def __init__(self) -> None:
        self.remaining: float | None = None
        self.reset: float = 0.0

    def observe(self, headers: Any) -> None:
        try:
            if (rem := headers.get("X-Ratelimit-Remaining")) is not None:
                self.remaining = float(rem)
            if (res := headers.get("X-Ratelimit-Reset")) is not None:
                self.reset = float(res)
        except (TypeError, ValueError):
            pass

    def wait(self) -> None:
        if self.remaining is not None and self.remaining < 2:
            nap = max(self.reset, 1.0) + 0.5
            print(f"  [rate limit] quota exhausted, sleeping {nap:.0f}s", file=sys.stderr)
            time.sleep(nap)
            self.remaining = None
        else:
            time.sleep(0.1)


_limiter = RateLimiter()


def get(path: str, params: dict[str, Any], retries: int = 5) -> list[dict]:
    """GET an Arctic Shift endpoint, retrying transient failures.

    The API intermittently answers a valid query with 422, so that status is
    retried rather than treated as a permanent rejection; a genuinely malformed
    query still surfaces once the attempts are spent.
    """
    url = f"{API}/{path}?{urllib.parse.urlencode(params)}"
    for attempt in range(retries):
        _limiter.wait()
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                _limiter.observe(resp.headers)
                return json.loads(resp.read().decode("utf-8")).get("data", [])
        except urllib.error.HTTPError as e:
            if e.code == 429:
                backoff = float(e.headers.get("X-Ratelimit-Reset") or 2**attempt)
                print(f"  [429] backing off {backoff:.0f}s", file=sys.stderr)
                time.sleep(backoff + 0.5)
                continue
            if e.code == 422 or e.code >= 500:
                if attempt == retries - 1:
                    raise RuntimeError(f"{e.code} from {url}: {e.read().decode()[:300]}") from e
                time.sleep(2**attempt)
                continue
            raise
        except (urllib.error.URLError, TimeoutError) as e:
            if attempt == retries - 1:
                raise
            print(f"  [network] {e}, retry {attempt + 1}/{retries}", file=sys.stderr)
            time.sleep(2**attempt)
    raise RuntimeError(f"giving up on {url} after {retries} attempts")


def paginate(path: str, params: dict[str, Any]) -> Iterator[dict]:
    """Yield every record for a query, ascending by created_utc.

    The API sorts descending by default and treats `after` as exclusive, so a
    naive cursor drops any record sharing the boundary second -- which does
    happen on real threads. We sort ascending, rewind the cursor one second
    each page, and dedupe by id, trading a little overlap for completeness.
    """
    seen: set[str] = set()
    # The caller's `after` may be a date string; it seeds the first page only.
    # From then on the cursor is a unix timestamp that supersedes it.
    cursor: int | None = None
    while True:
        query = {**params, "limit": PAGE_LIMIT, "sort": "asc"}
        if cursor is not None:
            query["after"] = cursor
        page = get(path, query)
        if not page:
            return

        fresh = [r for r in page if r["id"] not in seen]
        for record in fresh:
            seen.add(record["id"])
            yield record

        if len(page) < PAGE_LIMIT:
            return

        last_ts = page[-1]["created_utc"]
        next_cursor = last_ts - 1

        # A full page sharing one timestamp would pin the cursor forever; step
        # past it and accept the loss rather than spin.
        if cursor is not None and next_cursor <= cursor and not fresh:
            print(
                f"  [warn] >{PAGE_LIMIT} records at created_utc={last_ts}; "
                f"skipping ahead, some may be missed",
                file=sys.stderr,
            )
            next_cursor = last_ts + 1
        cursor = next_cursor


def db_connect(path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    return conn


def save_comments(conn: sqlite3.Connection, rows: list[dict]) -> None:
    conn.executemany(
        """INSERT INTO comments (id, link_id, parent_id, subreddit, author, body,
                                 created_utc, score, permalink, raw)
           VALUES (:id, :link_id, :parent_id, :subreddit, :author, :body,
                   :created_utc, :score, :permalink, :raw)
           ON CONFLICT(id) DO UPDATE SET
               score = excluded.score, body = excluded.body, raw = excluded.raw""",
        [
            {
                "id": c["id"],
                "link_id": c.get("link_id", ""),
                "parent_id": c.get("parent_id", ""),
                "subreddit": c.get("subreddit"),
                "author": c.get("author"),
                "body": c.get("body"),
                "created_utc": c.get("created_utc"),
                "score": c.get("score"),
                "permalink": c.get("permalink"),
                "raw": json.dumps(c, separators=(",", ":")),
            }
            for c in rows
        ],
    )
    conn.commit()


def save_posts(conn: sqlite3.Connection, rows: list[dict]) -> None:
    conn.executemany(
        """INSERT INTO posts (id, subreddit, title, author, created_utc, score,
                              num_comments, permalink, selftext, raw)
           VALUES (:id, :subreddit, :title, :author, :created_utc, :score,
                   :num_comments, :permalink, :selftext, :raw)
           ON CONFLICT(id) DO UPDATE SET
               score = excluded.score, num_comments = excluded.num_comments,
               raw = excluded.raw""",
        [
            {
                "id": p["id"],
                "subreddit": p.get("subreddit"),
                "title": p.get("title"),
                "author": p.get("author"),
                "created_utc": p.get("created_utc"),
                "score": p.get("score"),
                "num_comments": p.get("num_comments"),
                "permalink": p.get("permalink"),
                "selftext": p.get("selftext"),
                "raw": json.dumps(p, separators=(",", ":")),
            }
            for p in rows
        ],
    )
    conn.commit()


def scrape_thread(conn: sqlite3.Connection, post_id: str, force: bool = False) -> int:
    """Fetch every comment on one post. Skips threads already stored."""
    link_id = post_id if post_id.startswith("t3_") else f"t3_{post_id}"
    bare = link_id[3:]

    if not force:
        row = conn.execute(
            "SELECT comment_count FROM fetched_threads WHERE link_id = ?", (link_id,)
        ).fetchone()
        if row:
            print(f"{bare}: already fetched ({row[0]} comments), skipping")
            return 0

    comments = list(paginate("comments/search", {"link_id": link_id}))
    if comments:
        save_comments(conn, comments)
    conn.execute(
        "INSERT INTO fetched_threads (link_id, comment_count, fetched_at) VALUES (?, ?, ?) "
        "ON CONFLICT(link_id) DO UPDATE SET comment_count = excluded.comment_count, "
        "fetched_at = excluded.fetched_at",
        (link_id, len(comments), int(time.time())),
    )
    conn.commit()
    print(f"{bare}: {len(comments)} comments")
    return len(comments)


def scrape_subreddit(conn: sqlite3.Connection, name: str, after: str, before: str,
                     min_comments: int, force: bool) -> None:
    """Fetch posts in a date window, then every comment tree among them."""
    print(f"r/{name}: listing posts {after} .. {before}")
    posts = [
        p for p in paginate("posts/search", {"subreddit": name, "after": after, "before": before})
        if p.get("num_comments", 0) >= min_comments
    ]
    if not posts:
        print("no posts matched")
        return
    save_posts(conn, posts)
    print(f"r/{name}: {len(posts)} posts with >= {min_comments} comments\n")

    total = 0
    failed = []
    for i, post in enumerate(posts, 1):
        print(f"[{i}/{len(posts)}] ", end="")
        try:
            total += scrape_thread(conn, post["id"], force=force)
        except (RuntimeError, urllib.error.HTTPError, urllib.error.URLError) as e:
            # One unreachable thread shouldn't abandon the rest of the crawl;
            # it stays unrecorded so a later run retries it.
            print(f"{post['id']}: FAILED ({e})", file=sys.stderr)
            failed.append(post["id"])
    print(f"\ndone: {total} comments from {len(posts) - len(failed)} threads -> {DB_PATH}")
    if failed:
        print(f"{len(failed)} failed, rerun to retry: {' '.join(failed)}", file=sys.stderr)


def build_tree(conn: sqlite3.Connection, post_id: str) -> list[dict]:
    """Rebuild the nested comment tree for a post from stored parent_id links."""
    link_id = post_id if post_id.startswith("t3_") else f"t3_{post_id}"
    rows = conn.execute(
        "SELECT id, parent_id, author, body, score, created_utc FROM comments "
        "WHERE link_id = ? ORDER BY created_utc",
        (link_id,),
    ).fetchall()

    nodes = {
        r[0]: {"id": r[0], "parent_id": r[1], "author": r[2], "body": r[3],
               "score": r[4], "created_utc": r[5], "replies": []}
        for r in rows
    }
    roots = []
    for node in nodes.values():
        parent = node["parent_id"]
        # t3_ parent means top-level; t1_ means a reply to another comment.
        if parent.startswith("t1_") and (p := nodes.get(parent[3:])):
            p["replies"].append(node)
        else:
            roots.append(node)
    return roots


def print_tree(nodes: list[dict], depth: int = 0) -> None:
    for n in nodes:
        body = " ".join((n["body"] or "").split())
        if len(body) > 100:
            body = body[:100] + "..."
        print(f"{'  ' * depth}- u/{n['author']} ({n['score']}): {body}")
        print_tree(n["replies"], depth + 1)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_post = sub.add_parser("post", help="scrape comment trees for specific post ids")
    p_post.add_argument("post_ids", nargs="+")
    p_post.add_argument("--force", action="store_true", help="refetch even if stored")

    p_sub = sub.add_parser("subreddit", help="scrape all comment trees in a date window")
    p_sub.add_argument("name")
    p_sub.add_argument("--after", required=True, help="YYYY-MM-DD")
    p_sub.add_argument("--before", required=True, help="YYYY-MM-DD")
    p_sub.add_argument("--min-comments", type=int, default=1)
    p_sub.add_argument("--force", action="store_true")

    p_tree = sub.add_parser("tree", help="print a stored thread as a nested tree")
    p_tree.add_argument("post_id")

    args = ap.parse_args()
    conn = db_connect()
    try:
        if args.cmd == "post":
            for pid in args.post_ids:
                scrape_thread(conn, pid, force=args.force)
        elif args.cmd == "subreddit":
            scrape_subreddit(conn, args.name, args.after, args.before,
                             args.min_comments, args.force)
        elif args.cmd == "tree":
            tree = build_tree(conn, args.post_id)
            if not tree:
                print("nothing stored for that post; run `post` first")
                return 1
            print_tree(tree)
    except KeyboardInterrupt:
        print("\ninterrupted; progress is saved, rerun to resume", file=sys.stderr)
        return 130
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
