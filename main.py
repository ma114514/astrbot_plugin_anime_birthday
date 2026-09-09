"""生日指令与每日推送；抓取、缓存及存储分别由独立模块负责。"""

import asyncio
import datetime as dt
import re
import sqlite3
from pathlib import Path

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import Node, Nodes, Plain
from astrbot.api.star import Context, Star, StarTools, register

if __package__:
    from .birthday_service import BirthdayService
    from .moegirl_api import MoegirlBirthdayFetcher
    from .storage import BirthdayEntry, BirthdayStore, date_key
else:
    from birthday_service import BirthdayService
    from moegirl_api import MoegirlBirthdayFetcher
    from storage import BirthdayEntry, BirthdayStore, date_key

PLUGIN_NAME = "astrbot_plugin_anime_birthday"
BEIJING = dt.timezone(dt.timedelta(hours=8))
FEED_INCLUDE = {
    "gal": {"gal", "gal_anime", "unknown"},
    "game": {"game", "game_anime", "unknown"},
    "anime": {"anime", "gal_anime", "game_anime", "unknown"},
    "all": {"gal", "gal_anime", "game", "game_anime", "anime", "other", "unknown"},
}
FEED_LABELS = {"gal": "Galgame", "game": "二次元游戏", "anime": "番剧", "all": "全部"}
FEED_ALIASES = {
    "gal": "gal",
    "galgame": "gal",
    "美少女游戏": "gal",
    "游戏": "game",
    "手游": "game",
    "二次元游戏": "game",
    "game": "game",
    "番剧": "anime",
    "动画": "anime",
    "动漫": "anime",
    "anime": "anime",
    "全部": "all",
    "都": "all",
    "all": "all",
}
FEED_HELP = "请选择类型：gal / 游戏 / 番剧 / 全部。例：/生日订阅 游戏"
SECTIONS = (
    ("🎮 Galgame 角色", {"gal", "gal_anime"}),
    ("📱 二次元游戏角色", {"game", "game_anime"}),
    ("📺 番剧角色", {"anime", "gal_anime", "game_anime"}),
    ("📦 其他角色", {"other", "unknown"}),
)


@register(
    PLUGIN_NAME,
    "ma114514",
    "本地缓存角色生日，按类型订阅每日推送",
    "2.0.0",
    "https://github.com/ma114514/astrbot_plugin_anime_birthday",
)
class AnimeBirthdayPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig | dict | None = None):
        super().__init__(context)
        self.config = config if config is not None else {}
        self._store: BirthdayStore | None = None
        self._service: BirthdayService | None = None
        self._subscriptions: dict[str, dict] = {}
        self._attempts: dict[str, tuple[str, int, float]] = {}
        self._sched_task: asyncio.Task | None = None
        self._initialize_lock = asyncio.Lock()
        self._state_lock = asyncio.Lock()
        self._push_lock = asyncio.Lock()
        self._wake = asyncio.Event()
        self._closed = False

    def _int_cfg(self, key: str, default: int, low: int, high: int) -> int:
        try:
            value = int(self.config.get(key, default))
        except (TypeError, ValueError, OverflowError):
            return default
        return min(high, max(low, value))

    def _bool_cfg(self, key: str, default: bool) -> bool:
        value = self.config.get(key, default)
        if isinstance(value, str):
            return value.strip().lower() in {"true", "1", "yes", "on"}
        return bool(value)

    def _now(self) -> dt.datetime:
        return dt.datetime.now(BEIJING)

    def _data_dir(self) -> Path:
        return Path(StarTools.get_data_dir(PLUGIN_NAME))

    async def initialize(self):
        async with self._initialize_lock:
            if self._service is not None or self._closed:
                return
            store = BirthdayStore(await asyncio.to_thread(self._data_dir))
            await asyncio.to_thread(store.initialize)
            subscriptions = await asyncio.to_thread(store.subscriptions)
            fetcher = MoegirlBirthdayFetcher(
                store=store,
                delay=self._int_cfg("request_interval_ms", 500, 500, 10000) / 1000,
                timeout=self._int_cfg("request_timeout", 20, 5, 120),
                work_ttl_days=self._int_cfg("work_cache_days", 180, 1, 3650),
            )
            self._store = store
            self._subscriptions = subscriptions
            self._service = BirthdayService(
                store,
                fetcher,
                ttl_days=self._int_cfg("birthday_cache_days", 30, 1, 3650),
                memory_days=self._int_cfg("memory_cache_days", 8, 1, 31),
            )
            self._sched_task = asyncio.create_task(
                self._scheduler_loop(), name=f"{PLUGIN_NAME}:scheduler"
            )
            logger.info(f"[{PLUGIN_NAME}] 本地生日缓存已就绪：{store.path}")

    async def terminate(self):
        self._closed = True
        async with self._initialize_lock:
            if self._sched_task is not None:
                self._sched_task.cancel()
                await asyncio.gather(self._sched_task, return_exceptions=True)
            if self._service is not None:
                await self._service.close()
        # 等待已有手动推送退出，避免卸载后仍向后续会话发送。
        async with self._push_lock:
            pass

    @staticmethod
    def _parse_feed(text: str, default: str = "all") -> str:
        if not text.strip():
            return default
        feed = FEED_ALIASES.get(text.strip().lower())
        if feed is None:
            raise ValueError(FEED_HELP)
        return feed

    def _parse_date(self, text: str) -> tuple[int, int]:
        if not text.strip():
            now = self._now()
            return now.month, now.day
        match = re.fullmatch(r"(\d{1,2})月(\d{1,2})日", text.strip())
        if match is None:
            match = re.fullmatch(r"(\d{1,2})[-/](\d{1,2})", text.strip())
        if match is None:
            raise ValueError("日期格式应为 3月8日、3-8 或 3/8。")
        month, day = map(int, match.groups())
        try:
            date_key(month, day)
        except ValueError:
            raise ValueError("日期无效，请检查月份和天数；支持 2月29日。") from None
        return month, day

    def _filter_characters(self, chars: list[dict], feed: str) -> list[dict]:
        require_origin = self._bool_cfg("only_with_origin", True)
        include = FEED_INCLUDE[feed]
        return [
            c
            for c in chars
            if (c.get("origin") or not require_origin)
            and (c.get("work_type") or "unknown") in include
        ]

    def _cache_note(self, entry: BirthdayEntry) -> str:
        if entry.fallback:
            return "提示：暂未取得完整的新数据，使用上次保存的本地生日数据。"
        if not entry.complete:
            return "提示：部分条目信息暂缺，下次查询或推送时会按需更新。"
        if not self._service.is_fresh(entry):
            return "提示：本次更新失败，使用上次保存的本地生日数据。"
        return ""

    @staticmethod
    def _character_line(c: dict) -> str:
        origin = f"（出自《{c['origin']}》）" if c.get("origin") else ""
        return f"{c['name']}{origin}"

    def _build_text(
        self, chars: list[dict], month: int, day: int, feed: str = "all", note: str = ""
    ) -> str:
        max_show = self._int_cfg("max_characters", 30, 1, 200)
        chars = self._filter_characters(chars, feed)
        if feed == "all":
            sections = [
                (title, [c for c in chars if (c.get("work_type") or "unknown") in kinds])
                for title, kinds in SECTIONS
            ]
        else:
            sections = [(f"{FEED_LABELS[feed]}角色", chars)]
        lines = [f"🎂 {month}月{day}日角色生日 🎂"]
        for title, items in sections:
            if not items:
                continue
            lines.extend(["", title])
            lines.extend(
                f"{i}. {self._character_line(c)}" for i, c in enumerate(items[:max_show], 1)
            )
            if len(items) > max_show:
                lines.append(f"……等共 {len(items)} 位")
        if not chars:
            lines.append(f"本地数据中没有符合「{FEED_LABELS[feed]}」条件的生日角色。")
        if note:
            lines.extend(["", note])
        lines.extend(["", "数据来源：萌娘百科"])
        return "\n".join(lines)

    def _build_forward_chain(
        self, chars: list[dict], month: int, day: int, note: str = ""
    ) -> MessageChain:
        limit = self._int_cfg("max_forward_characters", 100, 1, 200)
        contents = [f"🎂 {month}月{day}日生日播报（共 {len(chars)} 位）"]
        contents.extend(
            f"[{c.get('work_type', 'unknown')}] {self._character_line(c)}" for c in chars[:limit]
        )
        if len(chars) > limit:
            contents.append(f"仅展示前 {limit} 位；共 {len(chars)} 位。")
        if note:
            contents.append(note)
        contents.append("数据来源：萌娘百科")
        nodes = [Node(uin=10000, name="角色生日播报", content=[Plain(text)]) for text in contents]
        return MessageChain(chain=[Nodes(nodes=nodes)])

    def _supports_forward(self, umo: str) -> bool:
        getter = getattr(self.context, "get_platform_inst", None)
        platform = getter(umo.split(":", 1)[0]) if getter else None
        return platform is not None and platform.meta().name == "aiocqhttp"

    async def _send_to_session(
        self, umo: str, chars: list[dict], month: int, day: int, text: str, note: str
    ) -> bool:
        try:
            if (
                self._bool_cfg("use_forward", False)
                and self._supports_forward(umo)
                and len(chars) > self._int_cfg("max_characters", 30, 1, 200)
            ):
                chain = self._build_forward_chain(chars, month, day, note)
            else:
                # 平台可能修改消息链，逐会话创建，只复用渲染后的文本。
                chain = MessageChain(chain=[Plain(text)])
            return bool(await asyncio.wait_for(self.context.send_message(umo, chain), timeout=60))
        except Exception as exc:  # noqa: BLE001 - 平台适配器的异常类型不统一。
            logger.warning(f"[{PLUGIN_NAME}] 推送到 {umo} 失败：{exc}")
            return False

    def _eligible(self, umo: str, info: dict, now: dt.datetime) -> bool:
        today = now.date().isoformat()
        if info.get("last_sent") == today:
            return False
        date, count, retry = self._attempts.get(umo, (today, 0, 0))
        return date != today or (count < 3 and now.timestamp() >= retry)

    async def _push_to_all(self, month: int, day: int, scheduled_date: str = "") -> tuple[int, int]:
        async with self._push_lock:
            now = self._now()
            subs = {
                umo: info
                for umo, info in self._subscriptions.items()
                if not scheduled_date or self._eligible(umo, info, now)
            }
            if not subs:
                return 0, 0
            if scheduled_date:
                for umo in subs:
                    old_day, count, _ = self._attempts.get(umo, (scheduled_date, 0, 0))
                    self._attempts[umo] = (
                        scheduled_date,
                        count + 1 if old_day == scheduled_date else 1,
                        now.timestamp() + 600,
                    )
            entry = await self._service.get(month, day)
            note = self._cache_note(entry)
            rendered = {}
            successful = 0
            for index, (umo, info) in enumerate(subs.items()):
                if index:
                    await asyncio.sleep(1)
                if self._closed:
                    break
                if scheduled_date and self._now().date().isoformat() != scheduled_date:
                    break  # 抓取跨过午夜后，不再发送昨天的生日。
                if self._subscriptions.get(umo) is not info:
                    continue  # 抓取期间退订或更改了偏好。
                feed = info["type"]
                if feed not in rendered:
                    chars = self._filter_characters(entry.characters, feed)
                    rendered[feed] = (chars, self._build_text(chars, month, day, feed, note))
                chars, text = rendered[feed]
                if await self._send_to_session(umo, chars, month, day, text, note):
                    successful += 1
                    if scheduled_date:
                        async with self._state_lock:
                            current = self._subscriptions.get(umo)
                            if current is not None:
                                current["last_sent"] = scheduled_date
                                try:
                                    await asyncio.to_thread(
                                        self._store.mark_sent, umo, scheduled_date
                                    )
                                except (OSError, sqlite3.Error):
                                    logger.exception(
                                        f"[{PLUGIN_NAME}] 推送成功但保存发送记录失败；重启后可能重发"
                                    )
            return successful, len(subs)

    def _next_delay(self, now: dt.datetime) -> float | None:
        if not self._subscriptions:
            return None
        target = now.replace(
            hour=self._int_cfg("send_hour", 8, 0, 23),
            minute=self._int_cfg("send_minute", 0, 0, 59),
            second=0,
            microsecond=0,
        )
        if now < target:
            return (target - now).total_seconds()
        waits = [(target + dt.timedelta(days=1) - now).total_seconds()]
        today = now.date().isoformat()
        for umo, info in self._subscriptions.items():
            if info.get("last_sent") == today:
                continue
            date, count, retry = self._attempts.get(umo, (today, 0, 0))
            if date != today or count < 3:
                waits.append(max(0, retry - now.timestamp()) if date == today else 0)
        return min(waits)

    async def _scheduler_loop(self):
        while not self._closed:
            self._wake.clear()
            try:
                now = self._now()
                delay = self._next_delay(now)
                if delay == 0:
                    ok, total = await self._push_to_all(now.month, now.day, now.date().isoformat())
                    logger.info(f"[{PLUGIN_NAME}] 每日推送：{ok}/{total} 个会话成功")
                    continue
                await asyncio.wait_for(self._wake.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - 独立定时任务必须记录异常并继续运行。
                logger.exception(f"[{PLUGIN_NAME}] 定时推送失败，稍后重试")
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=600)
                except asyncio.TimeoutError:
                    pass

    def _check_admin(self, event: AstrMessageEvent) -> bool:
        return not self._bool_cfg("admin_only", True) or event.is_admin()

    async def _query_text(self, date: str, feed: str, force: bool = False) -> str:
        try:
            month, day = self._parse_date(date)
            preference = self._parse_feed(feed)
            entry = await self._service.get(month, day, force=force)
            return self._build_text(
                entry.characters, month, day, preference, self._cache_note(entry)
            )
        except (ValueError, ConnectionError) as exc:
            return f"查询失败：{exc}"
        except (OSError, sqlite3.Error):
            logger.exception(f"[{PLUGIN_NAME}] 本地生日缓存读写失败")
            return "本地生日缓存读写失败，请检查数据目录权限和磁盘空间。"

    @filter.command("今日生日")
    async def today_birthday(self, event: AstrMessageEvent, feed_str: str = ""):
        """查询今天生日，可选 gal / 游戏 / 番剧 / 全部。"""
        yield event.plain_result(await self._query_text("", feed_str))

    @filter.command("生日查询")
    async def query_birthday(self, event: AstrMessageEvent, date_str: str = "", feed_str: str = ""):
        """查询指定日期，例如 /生日查询 3月8日 番剧；优先读取本地缓存。"""
        yield event.plain_result(await self._query_text(date_str, feed_str))

    @filter.command("生日刷新")
    async def refresh_birthday(self, event: AstrMessageEvent, date_str: str = ""):
        """更新指定日期的生日缓存，省略日期则更新今天。"""
        if not self._check_admin(event):
            yield event.plain_result("只有管理员可以刷新生日缓存。")
            return
        yield event.plain_result(await self._query_text(date_str, "all", force=True))

    @filter.command("生日缓存")
    async def cache_status(self, event: AstrMessageEvent):
        """查看本地生日缓存规模。"""
        if not self._check_admin(event):
            yield event.plain_result("只有管理员可以查看生日缓存状态。")
            return
        stats = await asyncio.to_thread(self._store.stats)
        yield event.plain_result(
            f"本地缓存：{stats['days']} 个日期，{stats['characters']} 条角色生日，"
            f"{stats['works']} 部作品分类；占用 {stats['bytes'] / 1024:.1f} KiB。\n"
            "发送 /生日刷新 3月8日 可更新指定日期。"
        )

    @filter.command("生日订阅")
    async def subscribe(self, event: AstrMessageEvent, feed_str: str = ""):
        """订阅每日推送：/生日订阅 gal|游戏|番剧|全部。"""
        if not self._check_admin(event):
            yield event.plain_result("只有管理员可以订阅每日生日推送。")
            return
        try:
            feed = self._parse_feed(feed_str, default="")
        except ValueError as exc:
            yield event.plain_result(str(exc))
            return
        if not feed:
            yield event.plain_result(FEED_HELP)
            return
        umo = event.unified_msg_origin
        async with self._state_lock:
            await asyncio.to_thread(self._store.subscribe, umo, feed)
            old = self._subscriptions.get(umo, {})
            self._subscriptions[umo] = {"type": feed, "last_sent": old.get("last_sent", "")}
        self._wake.set()
        hour = self._int_cfg("send_hour", 8, 0, 23)
        minute = self._int_cfg("send_minute", 0, 0, 59)
        yield event.plain_result(
            f"已订阅「{FEED_LABELS[feed]}」角色生日，每天 {hour:02d}:{minute:02d}（北京时间）推送。\n"
            "发送 /生日退订 可取消订阅。"
        )

    @filter.command("生日退订")
    async def unsubscribe(self, event: AstrMessageEvent):
        """取消当前会话的生日推送。"""
        if not self._check_admin(event):
            yield event.plain_result("只有管理员可以退订每日生日推送。")
            return
        umo = event.unified_msg_origin
        async with self._state_lock:
            existed = umo in self._subscriptions
            await asyncio.to_thread(self._store.unsubscribe, umo)
            self._subscriptions.pop(umo, None)
            self._attempts.pop(umo, None)
        self._wake.set()
        yield event.plain_result("已退订每日生日推送。" if existed else "本会话尚未订阅。")

    @filter.command("生日订阅列表")
    async def sub_list(self, event: AstrMessageEvent):
        """查看已订阅会话及类型。"""
        if not self._check_admin(event):
            yield event.plain_result("只有管理员可以查看订阅列表。")
            return
        lines = [f"共 {len(self._subscriptions)} 个会话已订阅："]
        lines.extend(
            f"{i}. [{FEED_LABELS[info['type']]}] {umo}"
            for i, (umo, info) in enumerate(self._subscriptions.items(), 1)
        )
        yield event.plain_result("\n".join(lines))

    @filter.command("生日测试")
    async def test_push(self, event: AstrMessageEvent):
        """立即推送到全部订阅会话；不改变每日自动推送记录。"""
        if not self._check_admin(event):
            yield event.plain_result("只有管理员可以执行生日测试。")
            return
        if self._push_lock.locked():
            yield event.plain_result("已有生日推送正在进行，请稍后再试。")
            return
        now = self._now()
        try:
            ok, total = await self._push_to_all(now.month, now.day)
            yield event.plain_result(f"测试推送完成：{ok}/{total} 个会话成功。")
        except (ConnectionError, OSError, sqlite3.Error) as exc:
            yield event.plain_result(f"测试推送失败：{exc}")
