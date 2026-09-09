"""本地 SQLite 存储。同步方法由调用方放入 asyncio.to_thread 执行。"""

import datetime as dt
import json
import logging
import re
import sqlite3
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)
WORK_TYPES = {"gal", "gal_anime", "game", "game_anime", "anime", "other", "unknown"}
FEEDS = {"gal", "game", "anime", "all"}


def date_key(month: int, day: int) -> str:
    # 使用闰年校验，允许查询 2 月 29 日。
    dt.date(2000, month, day)
    return f"{month:02d}-{day:02d}"


def clean_characters(items: object) -> list[dict]:
    if not isinstance(items, list):
        raise TypeError("角色缓存必须是列表")
    result = []
    for item in items:
        if not isinstance(item, dict) or not isinstance(item.get("name"), str):
            raise TypeError("角色缓存格式无效")
        result.append(
            {
                "name": item["name"],
                "origin": item.get("origin") if isinstance(item.get("origin"), str) else "",
                "url": item.get("url") if isinstance(item.get("url"), str) else "",
                "work_type": item.get("work_type")
                if isinstance(item.get("work_type"), str) and item.get("work_type") in WORK_TYPES
                else "unknown",
            }
        )
    return result


@dataclass(frozen=True)
class BirthdayEntry:
    characters: list[dict]
    fetched_at: float
    complete: bool = True
    fallback: bool = field(default=False, compare=False)


class BirthdayStore:
    def __init__(self, data_dir: str | Path):
        self.data_dir = Path(data_dir)
        self.path = self.data_dir / "birthdays.sqlite3"

    def _connect(self):
        # 每次操作独立连接，不在默认线程池之间共享连接或长期占用文件句柄。
        return sqlite3.connect(self.path, timeout=10)

    def initialize(self):
        self.data_dir.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as db, db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS birthdays (
                    day TEXT PRIMARY KEY, characters TEXT NOT NULL,
                    fetched_at REAL NOT NULL, complete INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS works (
                    origin TEXT PRIMARY KEY, work_type TEXT NOT NULL,
                    fetched_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS subscriptions (
                    umo TEXT PRIMARY KEY, feed TEXT NOT NULL, last_sent TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            """)
            if not db.execute("SELECT 1 FROM metadata WHERE key='legacy_imported'").fetchone():
                if self._import_legacy(db):
                    db.execute("INSERT INTO metadata VALUES ('legacy_imported', '1')")

    def _import_legacy(self, db):
        path = self.data_dir / "state.json"
        if not path.exists():
            return True
        try:
            state = json.loads(path.read_text(encoding="utf-8-sig"))
            if not isinstance(state, dict):
                raise TypeError("旧状态不是对象")
        except (ValueError, TypeError, OSError) as exc:
            # 不覆盖旧文件；修复后下次加载自动重试迁移。
            logger.warning("旧版 state.json 读取失败，保留原文件: %s", exc)
            return False
        subs = state.get("subscriptions", {})
        if isinstance(subs, list):
            subs = {s: {"type": "all"} for s in subs if isinstance(s, str)}
        last_sent = state.get("last_sent", "")
        if not isinstance(last_sent, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", last_sent):
            last_sent = ""
        for umo, info in subs.items() if isinstance(subs, dict) else []:
            if not isinstance(umo, str) or not umo:
                continue
            feed = info.get("type", "all") if isinstance(info, dict) else "all"
            db.execute(
                "INSERT OR IGNORE INTO subscriptions VALUES (?, ?, ?)",
                (umo, feed if isinstance(feed, str) and feed in FEEDS else "all", last_sent),
            )
        cache = state.get("cache", {})
        for key, characters in cache.items() if isinstance(cache, dict) else []:
            match = re.fullmatch(r"v3-(\d{1,2})-(\d{1,2})", key)
            if not match:
                continue
            try:
                day = date_key(*map(int, match.groups()))
                characters = clean_characters(characters)
            except (ValueError, TypeError):
                continue
            # 旧结果可能已过滤出处，且无可靠抓取时间。作为离线后备，首次使用时更新。
            db.execute(
                "INSERT OR IGNORE INTO birthdays VALUES (?, ?, 0, 0)",
                (day, json.dumps(characters, ensure_ascii=False)),
            )
        works = state.get("work_cache", {})
        modern = isinstance(cache, dict) and any(k.startswith("v3-") for k in cache)
        for origin, kind in works.items() if isinstance(works, dict) else []:
            if not isinstance(origin, str) or not isinstance(kind, str) or kind not in WORK_TYPES:
                continue
            if kind == "gal" and not modern:  # v1.1 的 gal 表示所有游戏。
                continue
            db.execute("INSERT OR IGNORE INTO works VALUES (?, ?, 0)", (origin, kind))
        return True

    def get_day(self, day: str) -> BirthdayEntry | None:
        with closing(self._connect()) as db:
            row = db.execute(
                "SELECT characters, fetched_at, complete FROM birthdays WHERE day=?", (day,)
            ).fetchone()
        if row is None:
            return None
        try:
            return BirthdayEntry(clean_characters(json.loads(row[0])), float(row[1]), bool(row[2]))
        except (ValueError, TypeError) as exc:
            logger.warning("忽略损坏的 %s 生日缓存: %s", day, exc)
            return None

    def put_day(self, day: str, entry: BirthdayEntry):
        payload = json.dumps(entry.characters, ensure_ascii=False, separators=(",", ":"))
        with closing(self._connect()) as db, db:
            db.execute(
                "INSERT OR REPLACE INTO birthdays VALUES (?, ?, ?, ?)",
                (day, payload, entry.fetched_at, int(entry.complete)),
            )

    def get_work(self, origin: str) -> tuple[str, float] | None:
        with closing(self._connect()) as db:
            return db.execute(
                "SELECT work_type, fetched_at FROM works WHERE origin=?", (origin,)
            ).fetchone()

    def put_work(self, origin: str, kind: str, fetched_at: float):
        with closing(self._connect()) as db, db:
            db.execute("INSERT OR REPLACE INTO works VALUES (?, ?, ?)", (origin, kind, fetched_at))

    def subscriptions(self) -> dict[str, dict]:
        with closing(self._connect()) as db:
            rows = db.execute("SELECT umo, feed, last_sent FROM subscriptions").fetchall()
        return {
            umo: {"type": feed if feed in FEEDS else "all", "last_sent": sent}
            for umo, feed, sent in rows
        }

    def subscribe(self, umo: str, feed: str):
        with closing(self._connect()) as db, db:
            db.execute(
                "INSERT INTO subscriptions (umo, feed) VALUES (?, ?) "
                "ON CONFLICT(umo) DO UPDATE SET feed=excluded.feed",
                (umo, feed),
            )

    def unsubscribe(self, umo: str):
        with closing(self._connect()) as db, db:
            db.execute("DELETE FROM subscriptions WHERE umo=?", (umo,))

    def mark_sent(self, umo: str, day: str):
        with closing(self._connect()) as db, db:
            db.execute("UPDATE subscriptions SET last_sent=? WHERE umo=?", (day, umo))

    def stats(self) -> dict:
        with closing(self._connect()) as db:
            days = characters = 0
            # 无需 SQLite 的可选 JSON 扩展；逐行读取，不把全年数据载入内存。
            for (payload,) in db.execute("SELECT characters FROM birthdays"):
                days += 1
                try:
                    items = json.loads(payload)
                    characters += len(items) if isinstance(items, list) else 0
                except ValueError:
                    continue
            works = db.execute("SELECT COUNT(*) FROM works").fetchone()[0]
        return {
            "days": days,
            "characters": characters,
            "works": works,
            "bytes": self.path.stat().st_size,
        }
