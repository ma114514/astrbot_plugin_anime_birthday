import asyncio
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock
from urllib.parse import quote

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from moegirl_api import CategoryParser, MoegirlBirthdayFetcher
from storage import BirthdayStore


def category(*titles, next_page="", previous=""):
    links = "".join(
        f'<li><a title="{title}" href="/character">{title}</a></li>' for title in titles
    )
    prev = f'<a href="?pagefrom={quote(previous)}">上一页</a>' if previous else ""
    nxt = (
        f'<a href="?title=Category:test&amp;pagefrom={quote(next_page)}">下一页</a>'
        if next_page
        else ""
    )
    return f'<div id="mw-pages">{prev}<div><ul>{links}</ul></div>{nxt}</div>'


class ParserTests(unittest.TestCase):
    def test_members_scope_entities_namespaces_and_title_spaces(self):
        parser = CategoryParser()
        parser.feed(
            '<li><a title="导航" href="/nav">导航</a></li>'
            + category("Love Live!:角色 A", "A &amp; B", "Template:生日", "A &amp; B")
            + '<div id="mw-subcategories"><li><a title="分类导航">分类导航</a></li></div>'
        )
        self.assertEqual(parser.members, ["Love Live!:角色 A", "A & B"])

    def test_intro_without_toc_and_preserves_title_spaces(self):
        html = '<div class="mw-parser-output"><p>角色是由某公司所制作的游戏《Love Live!》及其衍生作品的登场角色。</p></div>'
        self.assertEqual(
            MoegirlBirthdayFetcher._extract_origin(html, "角色"), ("Love Live!", "游戏")
        )

    def test_intro_excludes_scripts_headings_and_footer(self):
        html = '<div class="mw-parser-output"><style>恋爱游戏</style><p>漫画作品</p><h2>其他</h2>TV动画</div>手机游戏'
        intro = MoegirlBirthdayFetcher._extract_intro(html)
        self.assertEqual(intro, "漫画作品")
        self.assertEqual(MoegirlBirthdayFetcher._classify_intro(intro, ""), "other")

    def test_game_anime_and_adult_rating_alone_is_not_gal(self):
        self.assertEqual(
            MoegirlBirthdayFetcher._classify_intro("手机游戏，并有TV动画。", ""), "game_anime"
        )
        self.assertEqual(
            MoegirlBirthdayFetcher._classify_intro("美少女游戏，并有TV动画。", ""), "gal_anime"
        )
        self.assertEqual(MoegirlBirthdayFetcher._classify_intro("R-18 漫画", ""), "other")


class FetcherTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fetcher = MoegirlBirthdayFetcher()
        self.addAsyncCleanup(self.fetcher.close)

    async def test_pagination_decodes_once_ignores_previous_and_deduplicates(self):
        self.fetcher._get_page = AsyncMock(
            side_effect=[
                category("角色 A", next_page="角色 B"),
                category("角色 A", "角色 B", previous="角色 A"),
            ]
        )
        self.assertEqual(await self.fetcher._get_category_members(9, 5), ["角色 A", "角色 B"])
        self.assertEqual(
            self.fetcher._get_page.call_args_list[1].args, ("Category:9月5日", "角色 B")
        )

    async def test_challenge_page_and_broken_pagination_raise(self):
        self.fetcher._get_page = AsyncMock(return_value="<html>请完成验证</html>")
        with self.assertRaises(ConnectionError):
            await self.fetcher._get_category_members(9, 5)
        self.fetcher._get_page = AsyncMock(side_effect=[category("角色", next_page="B"), None])
        with self.assertRaises(ConnectionError):
            await self.fetcher._get_category_members(9, 5)

    async def test_cyclic_pagination_is_rejected(self):
        self.fetcher._get_page = AsyncMock(return_value=category("角色", next_page="B"))
        with self.assertRaises(ConnectionError):
            await self.fetcher._get_category_members(9, 5)

    async def test_real_empty_category_is_valid(self):
        self.fetcher._get_page = AsyncMock(
            return_value='<div class="mw-category-empty">空分类</div>'
        )
        self.assertEqual(await self.fetcher._get_category_members(2, 29), [])

    async def test_prefix_skips_character_request_and_shared_work_persists(self):
        with tempfile.TemporaryDirectory() as directory:
            store = BirthdayStore(directory)
            store.initialize()
            self.fetcher._store = store
            self.fetcher._get_page = AsyncMock(
                side_effect=[
                    category("作品:角色 A", "作品:角色 B"),
                    '<div class="mw-parser-output">手机游戏，改编TV动画。</div>',
                ]
            )
            result = await self.fetcher.fetch_birthdays(9, 5)
            self.assertTrue(result.complete)
            self.assertEqual(len(result.characters), 2)
            self.assertEqual(self.fetcher._get_page.await_count, 2)
            restarted = MoegirlBirthdayFetcher(store=store)
            restarted._get_page = AsyncMock(side_effect=AssertionError("cached work requested"))
            self.assertEqual(await restarted.classify_work("作品"), ("game_anime", True))
            await restarted.close()

    async def test_unknown_work_does_not_poison_persistent_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            store = BirthdayStore(directory)
            store.initialize()
            self.fetcher._store = store
            self.fetcher._get_page = AsyncMock(side_effect=ConnectionError("offline"))
            self.assertEqual(await self.fetcher.classify_work("作品"), ("unknown", False))
            self.assertIsNone(store.get_work("作品"))
            self.assertEqual(await self.fetcher.classify_work("作品"), ("unknown", False))
            self.fetcher._get_page.assert_awaited_once()

    async def test_http_forbidden_and_rate_limit_raise_and_stop_followups(self):
        for status in (403, 429):
            self.fetcher._blocked_until = 0
            self.fetcher._next_request = 0
            response = Mock(status=status)
            request = AsyncMock()
            request.__aenter__.return_value = response
            session = Mock()
            session.get.return_value = request
            self.fetcher._get_session = AsyncMock(return_value=session)
            with self.assertRaises(ConnectionError):
                await self.fetcher._get_page("Category:9月5日")
            with self.assertRaises(ConnectionError):
                await self.fetcher._get_page("Category:9月6日")
            session.get.assert_called_once()

    async def test_global_rate_limit_applies_to_all_requests(self):
        calls = []

        async def enter():
            calls.append(time.monotonic())
            return Mock(status=404)

        request = AsyncMock()
        request.__aenter__.side_effect = enter
        session = Mock()
        session.get.return_value = request
        self.fetcher._get_session = AsyncMock(return_value=session)
        await asyncio.gather(self.fetcher._get_page("A"), self.fetcher._get_page("B"))
        self.assertGreaterEqual(calls[1] - calls[0], 0.48)


if __name__ == "__main__":
    unittest.main()
