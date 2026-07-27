#!/usr/bin/env python3
"""Daily driver: search + harvest + write a factual digest.

Runs unattended (e.g. from Task Scheduler). Searches a fixed topic set over a
trailing window, fetches comment trees for the busiest threads, then writes a
plain-data digest to digests/YYYY-MM-DD.md. No commentary — top posts by
engagement and top comments by concrete-signal score, with links.
"""

from __future__ import annotations

import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import reddit_scraper as rs  # noqa: E402

# Edit this list to change what gets tracked. Each entry is a search phrase.
TOPICS = [
    "kimi", "qwen", "glm", "deepseek", "claude code",
    "codex", "gpt-5", "gemini", "opencode", "cursor",
]
SUBREDDITS = ["LocalLLaMA", "ClaudeAI", "singularity", "OpenAI", "ChatGPT",
              "ExperiencedDevs"]

WINDOW_DAYS = 3          # trailing window to search each run
MAX_PER_SUB = 60         # cap results per subreddit per topic
HARVEST_THREADS = 25     # busiest threads to pull comment trees for
MIN_COMMENTS = 15        # a thread must have at least this many comments
TOP_POSTS = 20           # posts listed in the digest
TOP_INSIGHTS = 25        # comments listed in the digest


def daterange() -> tuple[str, str]:
    today = datetime.date.today()
    after = today - datetime.timedelta(days=WINDOW_DAYS)
    return after.isoformat(), (today + datetime.timedelta(days=1)).isoformat()


def write_digest(conn, after: str, before: str) -> Path:
    """Write a digest restricted to posts created within the search window.

    Queries filter on created_utc so the numbers describe the window, not the
    entire accumulated database.
    """
    out_dir = Path(__file__).parent / "digests"
    out_dir.mkdir(exist_ok=True)
    path = out_dir / f"{datetime.date.today().isoformat()}.md"

    start = int(datetime.datetime.fromisoformat(after).replace(
        tzinfo=datetime.UTC).timestamp())

    lines: list[str] = []
    w = lines.append
    w(f"# Reddit AI digest — {datetime.date.today().isoformat()}")
    w(f"\nWindow: {after} .. {before}  |  Topics: {', '.join(TOPICS)}\n")

    # Per-topic engagement within the window only.
    w("## Topic engagement (posts created in window)\n")
    w("| topic | posts | upvotes | comments | top |")
    w("|---|---|---|---|---|")
    for r in conn.execute(
        """SELECT t.topic, COUNT(*), SUM(p.score), SUM(p.num_comments), MAX(p.score)
           FROM post_topics t JOIN posts p ON p.id = t.post_id
           WHERE p.created_utc >= ?
           GROUP BY t.topic ORDER BY SUM(p.score) DESC""", (start,)):
        w(f"| {r[0]} | {r[1]} | {r[2] or 0} | {r[3] or 0} | {r[4] or 0} |")

    # Top posts created in the window, by engagement.
    w("\n## Top posts by engagement\n")
    for r in conn.execute(
        """SELECT p.subreddit, p.title, p.score, p.num_comments, p.created_utc,
                  p.permalink, p.score_mature,
                  (SELECT GROUP_CONCAT(topic) FROM post_topics WHERE post_id = p.id)
           FROM posts p
           WHERE p.created_utc >= ?
             AND p.id IN (SELECT post_id FROM post_topics)
           ORDER BY (p.score + p.num_comments * 2) DESC
           LIMIT ?""", (start, TOP_POSTS)):
        when = datetime.datetime.fromtimestamp(r[4], datetime.UTC).strftime("%m-%d")
        flag = "" if r[6] else " *(score unsettled)*"
        w(f"- **{r[2]}↑ {r[3]}c** {when} r/{r[0]} [{r[7]}] — {r[1]}{flag}  ")
        w(f"  https://reddit.com{r[5]}")

    # Highest-signal comments on window posts (concrete detail, not popularity).
    w("\n## Highest-signal comments\n")
    rows = conn.execute(
        """SELECT c.body, c.score, c.subreddit, c.permalink, p.title
           FROM comments c JOIN posts p ON p.id = SUBSTR(c.link_id, 4)
           WHERE p.created_utc >= ?""", (start,)).fetchall()
    scored = []
    for body, score, sr, perm, title in rows:
        if not body or len(body.split()) < 30:
            continue
        s = rs.signal_score({"body": body, "score": score, "depth": 1})
        if s > 0:
            scored.append((s, body, score, sr, perm, title))
    scored.sort(key=lambda x: -x[0])
    for s, body, score, sr, perm, title in scored[:TOP_INSIGHTS]:
        txt = " ".join(body.split())
        if len(txt) > 500:
            txt = txt[:500] + "..."
        w(f"- **r/{sr}** ({score}↑) on *{(title or '?')[:60]}*  ")
        w(f"  {txt}  ")
        w(f"  https://reddit.com{perm}")

    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def main() -> int:
    sys.stdout.reconfigure(line_buffering=True, encoding="utf-8", errors="replace")

    # Runs from both a fixed-time trigger and an at-logon trigger, so it may
    # fire several times a day. Skip if today's digest already exists, unless
    # --force. This makes an at-logon trigger safe: the first launch of the day
    # produces the digest, later ones exit immediately.
    today_file = Path(__file__).parent / "digests" / f"{datetime.date.today().isoformat()}.md"
    if today_file.exists() and "--force" not in sys.argv:
        print(f"today's digest already exists ({today_file.name}); skipping")
        return 0

    after, before = daterange()
    conn = rs.db_connect()
    try:
        print(f"[{datetime.datetime.now():%H:%M}] searching {after}..{before}")
        for topic in TOPICS:
            n = rs.search_topic(conn, topic, SUBREDDITS, after, before,
                                max_per_sub=MAX_PER_SUB, exact=True)
            print(f"  {topic}: {n}")
        print("harvesting busiest threads")
        rs.harvest_comments(conn, None, HARVEST_THREADS, MIN_COMMENTS,
                            technical_only=False)
        path = write_digest(conn, after, before)
        print(f"digest -> {path}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
