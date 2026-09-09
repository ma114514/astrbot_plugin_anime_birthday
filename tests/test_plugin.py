"""离线接口替身验证指令与调度；不会连接或向真实平台发送消息。"""

import asyncio
import datetime as dt
import importlib.util
import logging
import sys
import tempfile
import time
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from test_cache import CHARACTERS

from storage import BirthdayEntry


class MessageChain:
    def __init__(self, chain):
        self.chain = chain


class Component:
    def __init__(self, text=None, **kwargs):
        self.text = text
        self.__dict__.update(kwargs)


class Star:
    def __init__(self, context):
        self.context = context


def load_plugin():
    modules = {
        name: types.ModuleType(name)
        for name in (
            "astrbot",
            "astrbot.api",
            "astrbot.api.event",
            "astrbot.api.message_components",
            "astrbot.api.star",
        )
    }
    api = modules["astrbot.api"]
    api.AstrBotConfig = dict
    api.logger = logging.getLogger("plugin-test")
    event = modules["astrbot.api.event"]
    event.AstrMessageEvent = object
    event.MessageChain = MessageChain
    event.filter = types.SimpleNamespace(command=lambda *args: lambda function: function)
    components = modules["astrbot.api.message_components"]
    components.Node = components.Nodes = components.Plain = Component
    star = modules["astrbot.api.star"]
    star.Star, star.Context = Star, object
    star.StarTools = object
    star.register = lambda *args: lambda cls: cls
    spec = importlib.util.spec_from_file_location("birthday_plugin_under_test", ROOT / "main.py")
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, modules):
        spec.loader.exec_module(module)
    return module


plugin_module = load_plugin()


class PluginTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.context = Mock()
        self.context.send_message = AsyncMock(return_value=True)
        self.context.get_platform_inst.return_value = None
        self.plugin = plugin_module.AnimeBirthdayPlugin(self.context, {})
        self.plugin._data_dir = lambda: Path(self.temp.name)
        self.now = dt.datetime(2026, 9, 5, 9, tzinfo=plugin_module.BEIJING)
        self.plugin._now = lambda: self.now
        await self.plugin.initialize()
        await asyncio.sleep(0)  # 让调度器在无订阅时进入等待。
        self.addAsyncCleanup(self.plugin.terminate)
        self.plugin._service.get = AsyncMock(return_value=BirthdayEntry(CHARACTERS, time.time()))

    def add_subscription(self, umo, feed="all", sent=""):
        self.plugin._store.subscribe(umo, feed)
        if sent:
            self.plugin._store.mark_sent(umo, sent)
        self.plugin._subscriptions[umo] = {"type": feed, "last_sent": sent}

    @staticmethod
    def event(admin=True, umo="qq:group:1"):
        event = Mock(unified_msg_origin=umo)
        event.is_admin.return_value = admin
        event.plain_result.side_effect = lambda text: text
        return event

    async def test_no_subscriptions_never_fetches_and_no_polling(self):
        self.assertIsNone(self.plugin._next_delay(self.now))
        self.assertEqual(await self.plugin._push_to_all(9, 5), (0, 0))
        self.plugin._service.get.assert_not_awaited()
        self.context.send_message.assert_not_awaited()
        self.assertFalse(self.plugin._sched_task.done())

    async def test_schedule_accepts_midnight_and_waits_until_tomorrow_when_sent(self):
        self.plugin.config["send_hour"] = 0
        self.add_subscription("one")
        midnight = self.now.replace(hour=0, minute=0)
        self.assertEqual(self.plugin._next_delay(midnight), 0)
        self.plugin._subscriptions["one"]["last_sent"] = "2026-09-05"
        self.assertEqual(self.plugin._next_delay(midnight), 86400)

    async def test_only_failed_session_is_retried_and_success_persists(self):
        self.add_subscription("one")
        self.add_subscription("two")
        self.context.send_message.side_effect = [True, RuntimeError("platform down"), True]
        self.assertEqual(await self.plugin._push_to_all(9, 5, "2026-09-05"), (1, 2))
        stored = self.plugin._store.subscriptions()
        self.assertEqual(stored["one"]["last_sent"], "2026-09-05")
        self.assertEqual(stored["two"]["last_sent"], "")
        self.assertEqual(self.plugin._next_delay(self.now), 600)
        self.now += dt.timedelta(minutes=10)
        self.assertEqual(await self.plugin._push_to_all(9, 5, "2026-09-05"), (1, 1))
        self.assertEqual(
            [call.args[0] for call in self.context.send_message.call_args_list],
            ["one", "two", "two"],
        )
        self.assertGreater(self.plugin._next_delay(self.now), 600)

    async def test_fetch_failure_never_marks_sent_and_retries_are_bounded(self):
        self.add_subscription("one")
        self.plugin._service.get.side_effect = ConnectionError("offline")
        for _ in range(3):
            with self.assertRaises(ConnectionError):
                await self.plugin._push_to_all(9, 5, "2026-09-05")
            self.now += dt.timedelta(minutes=10)
        self.assertEqual(self.plugin._store.subscriptions()["one"]["last_sent"], "")
        self.assertGreater(self.plugin._next_delay(self.now), 600)
        self.context.send_message.assert_not_awaited()

    async def test_filter_change_reuses_full_unfiltered_cache(self):
        self.plugin.config["only_with_origin"] = True
        text = await self.plugin._query_text("9月5日", "all")
        self.assertNotIn("角色 B", text)
        self.plugin.config["only_with_origin"] = False
        text = await self.plugin._query_text("9月5日", "all")
        self.assertIn("角色 B", text)
        self.assertEqual(len(self.plugin._service.get.return_value.characters), 2)

    async def test_invalid_date_and_feed_do_not_access_cache(self):
        for date in ("2月30日", "13-1", "hello", "3月8日垃圾"):
            self.assertIn("查询失败", await self.plugin._query_text(date, "all"))
        self.assertIn("请选择类型", await self.plugin._query_text("9月5日", "bad"))
        self.plugin._service.get.assert_not_awaited()
        self.assertEqual(self.plugin._parse_date("2/29"), (2, 29))

    async def test_query_uses_cache_and_refresh_requires_admin(self):
        result = [text async for text in self.plugin.query_birthday(self.event(), "3月8日", "游戏")]
        self.assertIn("3月8日", result[0])
        self.plugin._service.get.assert_awaited_once_with(3, 8, force=False)
        self.plugin._service.get.reset_mock()
        result = [
            text async for text in self.plugin.refresh_birthday(self.event(admin=False), "3月8日")
        ]
        self.assertIn("只有管理员", result[0])
        self.plugin._service.get.assert_not_awaited()

    async def test_no_match_send_error_is_contained(self):
        self.add_subscription("one", "gal")
        self.context.send_message.side_effect = RuntimeError("offline")
        self.assertEqual(await self.plugin._push_to_all(9, 5, "2026-09-05"), (0, 1))
        text = self.context.send_message.call_args.args[1].chain[0].text
        self.assertIn("没有符合", text)
        self.assertEqual(self.plugin._store.subscriptions()["one"]["last_sent"], "")

    async def test_unsubscribe_during_fetch_prevents_delivery(self):
        self.add_subscription("one")

        async def fetch(*args):
            self.plugin._subscriptions.pop("one")
            return BirthdayEntry(CHARACTERS, time.time())

        self.plugin._service.get.side_effect = fetch
        await self.plugin._push_to_all(9, 5)
        self.context.send_message.assert_not_awaited()

    async def test_fetch_across_midnight_does_not_send_yesterday(self):
        self.add_subscription("one")

        async def fetch(*args):
            self.now += dt.timedelta(days=1)
            return BirthdayEntry(CHARACTERS, time.time())

        self.plugin._service.get.side_effect = fetch
        await self.plugin._push_to_all(9, 5, "2026-09-05")
        self.context.send_message.assert_not_awaited()

    async def test_forward_is_bounded_and_unsupported_platform_uses_text(self):
        self.plugin.config.update(use_forward=True, max_characters=1, max_forward_characters=2)
        chain = self.plugin._build_forward_chain(CHARACTERS * 10, 9, 5)
        self.assertEqual(len(chain.chain[0].nodes), 5)
        self.add_subscription("one")
        await self.plugin._push_to_all(9, 5)
        self.assertIsNotNone(self.context.send_message.call_args.args[1].chain[0].text)

    async def test_test_push_does_not_change_daily_delivery_state(self):
        self.add_subscription("one")
        await self.plugin._push_to_all(9, 5)
        self.assertEqual(self.plugin._store.subscriptions()["one"]["last_sent"], "")

    async def test_initialize_is_idempotent_and_terminate_waits_for_scheduler(self):
        task = self.plugin._sched_task
        await self.plugin.initialize()
        self.assertIs(task, self.plugin._sched_task)
        await self.plugin.terminate()
        self.assertTrue(task.done())


if __name__ == "__main__":
    unittest.main()
