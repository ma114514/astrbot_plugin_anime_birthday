"""萌娘百科抓取：单连接限速、正文解析、作品分类缓存。仅依赖 aiohttp。"""

import asyncio
import json
import logging
import re
import time
from collections import OrderedDict
from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import parse_qs, quote, urlparse

import aiohttp

if __package__:
    from .storage import BirthdayStore, date_key
else:
    from storage import BirthdayStore, date_key

logger = logging.getLogger(__name__)
BASE_URL = "https://zh.moegirl.org.cn"
USER_AGENT = "AstrBot-AnimeBirthday/2.0 (birthday cache; low-frequency requests)"
SKIP_NAMESPACES = {
    "user",
    "user_talk",
    "template",
    "template_talk",
    "category",
    "help",
    "talk",
    "file",
    "module",
    "mediawiki",
    "萌娘百科",
    "萌娘百科_talk",
    "分类",
    "模板",
    "用户",
    "文件",
    "帮助",
    "讨论",
}
ORIGIN_WITH_KIND = re.compile(
    r"是(?:由)?.{0,60}?(?:所)?(?:制作|创作|开发)的(.{0,12}?)《([^《》]+)》"
)
ORIGIN_PATTERNS = tuple(
    map(
        re.compile,
        (
            r"是《([^《》]+)》",
            r"《([^《》]+)》及其衍生作品的登场角色",
            r"《([^《》]+)》(?:动画|漫画|游戏|小说)?(?:系列|衍生)",
            r"(?:登场于|出自)(?:游戏|动画|漫画|小说)?《([^《》]+)》",
        ),
    )
)
GAL_EVIDENCE = re.compile(
    r"galgame|ギャルゲー?|美少女游戏|EOCS|拔作|工口游戏|H游戏|恋爱(?:冒险)?游戏|百合游戏|纯爱游戏",
    re.I,
)
GAME_EVIDENCE = re.compile(
    r"电子游戏|视觉小说|(?:制作|创作|开发)的(?:手机|网络)?游戏|是一款.{0,15}游戏|类.{0,6}游戏[，。]|游戏作品|手机游戏|网络游戏|网页游戏|PC游戏|主机游戏|乙女游戏|美少女游戏|恋爱(?:冒险)?游戏|冒险游戏|模拟游戏|策略游戏|卡牌游戏|养成游戏|ADV游戏|RPG|平台\s*[A-Za-z0-9]",
    re.I,
)
ANIME_EVIDENCE = re.compile(
    r"并有[^。]{0,60}动画|改编载体[^。]{0,60}动画|TV动画|改编动画|动画化|动画版|[，、]动画[、等，。]|动画电影|剧场版动画|电视动画|动画作品|首播时间|放送时间|播出时间|话数|集数\s*[0-9１-９]",
    re.I,
)
FRANCHISE_GAME = re.compile(r"详见「[^」]*游戏」|[（(]游戏[)）]")
FRANCHISE_ANIME = re.compile(r"详见「[^」]*动画[」/]|电视动画|TV动画", re.I)


class CategoryParser(HTMLParser):
    """限定 mw-pages 范围，保留标题空格，正确解码向后翻页参数。"""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.depth = 0
        self.root = None
        self.found = False
        self.empty = False
        self.members: list[str] = []
        self.next_page = ""
        self._seen: set[str] = set()
        self._li = 0
        self._link = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if "mw-category-empty" in attrs.get("class", "").split():
            self.empty = True
        if tag == "div":
            self.depth += 1
            if attrs.get("id") == "mw-pages":
                self.root = self.depth
                self.found = True
        if self.root is None:
            return
        if tag == "li":
            self._li += 1
        if tag == "a":
            title = attrs.get("title", "").strip()
            if self._li and title and title not in self._seen:
                namespace = title.partition(":")[0].replace(" ", "_").lower()
                if namespace not in SKIP_NAMESPACES:
                    self.members.append(title)
                    self._seen.add(title)
            self._link = [attrs.get("href", ""), attrs.get("rel", ""), ""]

    def handle_data(self, data):
        if self._link is not None:
            self._link[2] += data

    def handle_endtag(self, tag):
        if tag == "a" and self._link is not None:
            href, rel, text = self._link
            query = parse_qs(urlparse(href).query)
            if "pagefrom" in query and (
                "next" in rel or re.search(r"下一|下\s*\d|next", text, re.I)
            ):
                self.next_page = query["pagefrom"][0]
            self._link = None
        if tag == "li":
            self._li = max(0, self._li - 1)
        if tag == "div":
            if self.depth == self.root:
                self.root = None
            self.depth = max(0, self.depth - 1)


class IntroParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.depth = 0
        self.root = None
        self.done = False
        self.ignored = 0
        self.parts: list[str] = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "div":
            self.depth += 1
            if not self.done and "mw-parser-output" in attrs.get("class", "").split():
                self.root = self.depth
        if self.root is not None:
            if (
                tag in ("h2", "h3")
                or attrs.get("id") == "toc"
                or attrs.get("property") == "mw:PageProp/toc"
            ):
                self.done = True
            if tag in ("script", "style"):
                self.ignored += 1

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self.ignored = max(0, self.ignored - 1)
        if tag == "div":
            if self.depth == self.root:
                self.done = True
                self.root = None
            self.depth = max(0, self.depth - 1)

    def handle_data(self, data):
        if self.root is not None and not self.done and not self.ignored:
            self.parts.append(data)


@dataclass(frozen=True)
class FetchResult:
    characters: list[dict]
    complete: bool = True


class MoegirlBirthdayFetcher:
    def __init__(
        self,
        delay: float = 0.5,
        timeout: float = 20,
        retries: int = 2,
        store: BirthdayStore | None = None,
        work_ttl_days: int = 180,
    ):
        self._delay = max(0.5, delay)
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._retries = retries
        self._session: aiohttp.ClientSession | None = None
        self._request_lock = asyncio.Lock()
        self._next_request = 0.0
        self._blocked_until = 0.0
        self._store = store
        self._work_ttl = work_ttl_days * 86400
        self._works: OrderedDict[str, tuple[str, float]] = OrderedDict()
        self._work_retry: OrderedDict[str, float] = OrderedDict()

    async def _get_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"User-Agent": USER_AGENT},
                timeout=self._timeout,
                connector=aiohttp.TCPConnector(limit=1, ttl_dns_cache=300),
            )
        return self._session

    async def close(self):
        if self._session is not None:
            await self._session.close()

    async def _request(self, endpoint: str, params: dict) -> str | None:
        """只有 404 代表不存在；拒绝访问、限流、超时均不能当作空数据。"""
        async with self._request_lock:
            if time.monotonic() < self._blocked_until:
                raise ConnectionError("数据源暂时不可用，稍后重试")
            error = "未知错误"
            for attempt in range(self._retries + 1):
                delay = self._next_request - time.monotonic()
                if delay > 0:
                    await asyncio.sleep(delay)
                self._next_request = time.monotonic() + self._delay
                try:
                    session = await self._get_session()
                    async with session.get(f"{BASE_URL}/{endpoint}", params=params) as response:
                        if response.status == 404:
                            return None
                        if response.status in (403, 429):
                            self._blocked_until = time.monotonic() + 600
                            raise ConnectionError(f"萌娘百科返回 HTTP {response.status}，暂缓请求")
                        if 400 <= response.status < 500:
                            raise ConnectionError(f"萌娘百科返回 HTTP {response.status}")
                        response.raise_for_status()
                        data = bytearray()
                        async for chunk in response.content.iter_chunked(65536):
                            data.extend(chunk)
                            if len(data) > 4 * 1024 * 1024:
                                raise ConnectionError("页面超过 4 MiB，停止解析")
                        return data.decode(response.charset or "utf-8", errors="replace")
                except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                    error = str(exc) or type(exc).__name__
                    if attempt < self._retries:
                        await asyncio.sleep(min(2**attempt, 8))
            self._blocked_until = time.monotonic() + 60
            raise ConnectionError(f"访问萌娘百科失败：{error}")

    async def _get_page(self, title: str, pagefrom: str = "") -> str | None:
        params = {"title": title}
        if pagefrom:
            params["pagefrom"] = pagefrom
        return await self._request("index.php", params)

    async def _get_category_members(self, month: int, day: int) -> list[str]:
        date_key(month, day)
        title = f"Category:{month}月{day}日"
        members: dict[str, None] = {}
        pagefrom = ""
        visited = {pagefrom}
        for _ in range(50):
            html = await self._get_page(title, pagefrom)
            if html is None:
                raise ConnectionError(f"生日分类 {month}月{day}日 不存在")
            parser = CategoryParser()
            await asyncio.to_thread(parser.feed, html)
            if not parser.found and not parser.empty:
                self._blocked_until = time.monotonic() + 600
                raise ConnectionError("未找到生日分类列表，可能是验证页面或网站结构变更")
            members.update(dict.fromkeys(parser.members))
            if not parser.next_page:
                return list(members)
            if parser.next_page in visited:
                raise ConnectionError("生日分类分页循环，未保存不完整数据")
            visited.add(parser.next_page)
            pagefrom = parser.next_page
        raise ConnectionError("生日分类超过分页上限，未保存不完整数据")

    @staticmethod
    def _page_url(title: str) -> str:
        return f"{BASE_URL}/{quote(title.replace(' ', '_'), safe='')}"

    @staticmethod
    def _extract_intro(html: str) -> str:
        parser = IntroParser()
        parser.feed(html)
        return re.sub(r"\s+", " ", "".join(parser.parts)).strip()

    @staticmethod
    def _origin_prefix(name: str) -> str:
        parts = re.split(r"[:：]", name, maxsplit=1)
        if len(parts) == 2 and parts[0] and not parts[0].isdigit():
            return parts[0].strip()
        return ""

    @classmethod
    def _extract_origin(cls, html: str, name: str) -> tuple[str, str]:
        prefix = cls._origin_prefix(name)
        if prefix:
            return prefix, ""
        intro = cls._extract_intro(html)
        match = ORIGIN_WITH_KIND.search(intro)
        if match:
            return match[2].strip(), match[1].strip()
        for pattern in ORIGIN_PATTERNS:
            match = pattern.search(intro)
            if match:
                return match[1].strip(), ""
        return "", ""

    async def _fetch_character(self, title: str) -> dict:
        name = title.strip()
        origin = self._origin_prefix(name)
        hint = ""
        if not origin:  # 标题已带作品名时，省去角色页请求。
            html = await self._get_page(title)
            if html is None or "mw-parser-output" not in html:
                raise ConnectionError(f"角色页面不可用：{title}")
            origin, hint = await asyncio.to_thread(self._extract_origin, html, name)
        return {"name": name, "origin": origin, "kind_hint": hint, "url": self._page_url(title)}

    async def _search_title(self, keyword: str) -> str | None:
        raw = await self._request(
            "api.php",
            {
                "action": "opensearch",
                "limit": 1,
                "format": "json",
                "redirects": "resolve",
                "search": keyword,
            },
        )
        try:
            data = json.loads(raw or "null")
            if isinstance(data, list) and len(data) > 1 and isinstance(data[1], list) and data[1]:
                return data[1][0] if isinstance(data[1][0], str) else None
        except ValueError:
            pass
        return None

    @staticmethod
    def _classify_intro(intro: str, hint: str) -> str:
        is_gal = bool(GAL_EVIDENCE.search(intro) or GAL_EVIDENCE.search(hint))
        is_game = bool(
            GAME_EVIDENCE.search(intro) or FRANCHISE_GAME.search(intro) or "游戏" in hint
        )
        anime = bool(
            ANIME_EVIDENCE.search(intro) or FRANCHISE_ANIME.search(intro) or "动画" in hint
        )
        if is_gal:
            return "gal_anime" if anime else "gal"
        if is_game:
            return "game_anime" if anime else "game"
        return "anime" if anime else "other"

    def _remember_work(self, origin: str, entry: tuple[str, float]):
        self._works[origin] = entry
        self._works.move_to_end(origin)
        while len(self._works) > 256:
            self._works.popitem(last=False)

    async def classify_work(self, origin: str, kind_hint: str = "") -> tuple[str, bool]:
        cached = self._works.get(origin)
        if cached is None and self._store is not None:
            cached = await asyncio.to_thread(self._store.get_work, origin)
            if cached:
                self._remember_work(origin, cached)
        if cached and cached[0] != "unknown" and 0 <= time.time() - cached[1] < self._work_ttl:
            return cached[0], True
        if time.monotonic() < self._work_retry.get(origin, 0):
            return (cached[0] if cached else "unknown"), False
        try:
            html = await self._get_page(origin)
            if html is None:
                suggestion = await self._search_title(origin)
                if suggestion and suggestion != origin:
                    html = await self._get_page(suggestion)
            intro = await asyncio.to_thread(self._extract_intro, html or "")
            if not intro:
                raise ConnectionError(f"作品正文不可用：{origin}")
        except ConnectionError as exc:
            logger.warning("作品分类暂不可用：%s", exc)
            self._work_retry[origin] = time.monotonic() + 600
            self._work_retry.move_to_end(origin)
            while len(self._work_retry) > 256:
                self._work_retry.popitem(last=False)
            return (cached[0] if cached else "unknown"), False
        kind = self._classify_intro(intro, kind_hint)
        stamp = time.time()
        if self._store is not None:
            await asyncio.to_thread(self._store.put_work, origin, kind, stamp)
        self._remember_work(origin, (kind, stamp))
        self._work_retry.pop(origin, None)
        return kind, True

    async def fetch_birthdays(self, month: int, day: int, limit: int | None = None) -> FetchResult:
        titles = await self._get_category_members(month, day)
        if limit is not None:
            titles = titles[: max(0, limit)]
        results = []
        complete = True
        fetched = 0
        for title in titles:
            try:
                char = await self._fetch_character(title)
                fetched += 1
            except ConnectionError as exc:
                logger.warning("角色抓取失败：%s", exc)
                complete = False
                char = {"name": title, "origin": "", "url": self._page_url(title), "kind_hint": ""}
            kind = "unknown"
            if char["origin"]:
                kind, classified = await self.classify_work(char["origin"], char["kind_hint"])
                complete = complete and classified
            results.append(
                {
                    "name": char["name"],
                    "origin": char["origin"],
                    "url": char["url"],
                    "work_type": kind,
                }
            )
        if titles and not fetched:
            raise ConnectionError("角色页面均获取失败，请稍后重试")
        return FetchResult(results, complete)

    async def get_birthday_characters(
        self, month: int, day: int, limit: int | None = None, drop_unknown_origin: bool = True
    ) -> list[dict]:
        result = await self.fetch_birthdays(month, day, limit)
        return [c for c in result.characters if c["origin"] or not drop_unknown_origin]


async def main():
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("month", type=int)
    parser.add_argument("day", type=int)
    parser.add_argument("limit", type=int, nargs="?", default=10)
    args = parser.parse_args()
    fetcher = MoegirlBirthdayFetcher()
    try:
        characters = await fetcher.get_birthday_characters(args.month, args.day, args.limit)
        print(json.dumps(characters, ensure_ascii=False, indent=2))
    finally:
        await fetcher.close()


if __name__ == "__main__":
    asyncio.run(main())
