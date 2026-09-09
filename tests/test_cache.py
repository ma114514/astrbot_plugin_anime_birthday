import asyncio
import json
import sqlite3
import sys
import tempfile
import time
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import AsyncMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from birthday_service import BirthdayService
from moegirl_api import FetchResult
from storage import BirthdayEntry, BirthdayStore, date_key

CHARACTERS = [
    {
        "name": "角色 A",
        "origin": "作品 A",
        "work_type": "game_anime",
        "url": "https://example.test/A",
    },
    {"name": "角色 B", "origin": "", "work_type": "unknown", "url": "https://example.test/B"},
]


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = BirthdayStore(self.temp.name)

    def test_legacy_migration_preserves_subscriptions_and_original(self):
        legacy = {
            "subscriptions": {"qq:group:1": {"type": "game"}, "broken": {"type": []}},
            "last_sent": "2026-09-05",
            "cache": {"v3-9-5": CHARACTERS, "v3-2-30": CHARACTERS},
            "work_cache": {"作品 A": "game_anime", "旧游戏": "gal", "过期分类": "both"},
        }
        path = Path(self.temp.name) / "state.json"
        original = json.dumps(legacy, ensure_ascii=False)
        path.write_text(original, encoding="utf-8")
        self.store.initialize()
        self.assertEqual(
            self.store.subscriptions()["qq:group:1"], {"type": "game", "last_sent": "2026-09-05"}
        )
        self.assertEqual(self.store.subscriptions()["broken"]["type"], "all")
        self.assertEqual(self.store.get_day("09-05").characters, CHARACTERS)
        self.assertEqual(self.store.get_day("09-05").fetched_at, 0)
        self.assertIsNone(self.store.get_day("02-30"))
        self.assertEqual(self.store.get_work("旧游戏")[0], "gal")
        self.assertIsNone(self.store.get_work("过期分类"))
        self.store.unsubscribe("qq:group:1")
        self.store.initialize()
        self.assertNotIn("qq:group:1", self.store.subscriptions())
        self.assertEqual(path.read_text(encoding="utf-8"), original)

    def test_list_subscriptions_and_corrupt_legacy(self):
        path = Path(self.temp.name) / "state.json"
        path.write_text(json.dumps({"subscriptions": ["one", "two", None]}), encoding="utf-8")
        self.store.initialize()
        self.assertEqual(set(self.store.subscriptions()), {"one", "two"})
        self.assertEqual(self.store.subscriptions()["one"]["type"], "all")
        with tempfile.TemporaryDirectory() as second:
            (Path(second) / "state.json").write_text("{bad", encoding="utf-8")
            store = BirthdayStore(second)
            store.initialize()
            self.assertEqual(store.subscriptions(), {})

    def test_corrupt_day_is_not_served_and_other_days_survive(self):
        self.store.initialize()
        self.store.put_day("09-05", BirthdayEntry(CHARACTERS, time.time()))
        with closing(sqlite3.connect(self.store.path)) as db, db:
            db.execute("INSERT INTO birthdays VALUES ('09-06', 'broken-json', 0, 1)")
        self.assertIsNone(self.store.get_day("09-06"))
        self.assertEqual(self.store.get_day("09-05").characters, CHARACTERS)

    def test_date_validation_and_leap_day(self):
        self.assertEqual(date_key(2, 29), "02-29")
        for month, day in ((2, 30), (4, 31), (0, 1), (13, 1), (1, 0)):
            with self.assertRaises(ValueError):
                date_key(month, day)

    def test_subscription_change_preserves_last_sent(self):
        self.store.initialize()
        self.store.subscribe("one", "all")
        self.store.mark_sent("one", "2026-09-05")
        self.store.subscribe("one", "gal")
        self.assertEqual(
            self.store.subscriptions()["one"], {"type": "gal", "last_sent": "2026-09-05"}
        )


class CacheTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = BirthdayStore(self.temp.name)
        self.store.initialize()
        self.fetcher = AsyncMock()
        self.fetcher.fetch_birthdays.return_value = FetchResult(CHARACTERS)
        self.service = BirthdayService(self.store, self.fetcher)
        self.addAsyncCleanup(self.service.close)

    async def test_parallel_queries_fetch_once_and_persist_across_restart(self):
        results = await asyncio.gather(*(self.service.get(9, 5) for _ in range(50)))
        self.assertTrue(all(r.characters == CHARACTERS for r in results))
        self.fetcher.fetch_birthdays.assert_awaited_once_with(9, 5)
        offline = AsyncMock()
        offline.fetch_birthdays.side_effect = ConnectionError("offline")
        restarted = BirthdayService(BirthdayStore(self.temp.name), offline)
        self.addAsyncCleanup(restarted.close)
        self.assertEqual((await restarted.get(9, 5)).characters, CHARACTERS)
        offline.fetch_birthdays.assert_not_awaited()

    async def test_empty_results_are_cached(self):
        self.fetcher.fetch_birthdays.return_value = FetchResult([])
        await self.service.get(2, 29)
        await self.service.get(2, 29)
        self.fetcher.fetch_birthdays.assert_awaited_once()
        self.assertEqual(self.store.get_day("02-29").characters, [])

    async def test_stale_cache_survives_network_failure_with_cooldown(self):
        old = BirthdayEntry(CHARACTERS, time.time() - 40 * 86400)
        self.store.put_day("09-05", old)
        self.fetcher.fetch_birthdays.side_effect = ConnectionError("offline")
        for _ in range(3):
            result = await self.service.get(9, 5)
            self.assertEqual(result, old)
        self.fetcher.fetch_birthdays.assert_awaited_once()
        self.assertEqual(self.store.get_day("09-05"), old)

    async def test_failure_without_cache_is_not_stored_as_empty(self):
        self.fetcher.fetch_birthdays.side_effect = ConnectionError("forbidden")
        for _ in range(2):
            with self.assertRaises(ConnectionError):
                await self.service.get(9, 5)
        self.assertIsNone(self.store.get_day("09-05"))
        self.fetcher.fetch_birthdays.assert_awaited_once()

    async def test_partial_refresh_does_not_replace_previous_data(self):
        old = BirthdayEntry(CHARACTERS, time.time() - 40 * 86400)
        self.store.put_day("09-05", old)
        self.fetcher.fetch_birthdays.return_value = FetchResult(CHARACTERS[:1], complete=False)
        self.assertEqual(await self.service.get(9, 5), old)
        self.assertEqual(self.store.get_day("09-05"), old)

    async def test_first_partial_result_has_short_expiry(self):
        self.fetcher.fetch_birthdays.return_value = FetchResult(CHARACTERS, complete=False)
        result = await self.service.get(9, 5)
        self.assertFalse(result.complete)
        self.assertTrue(self.service.is_fresh(result))
        self.assertFalse(self.service.is_fresh(BirthdayEntry(CHARACTERS, time.time() - 601, False)))

    async def test_cancelling_one_waiter_does_not_cancel_shared_fetch(self):
        started, release = asyncio.Event(), asyncio.Event()

        async def fetch(*args):
            started.set()
            await release.wait()
            return FetchResult(CHARACTERS)

        self.fetcher.fetch_birthdays.side_effect = fetch
        one = asyncio.create_task(self.service.get(9, 5))
        await started.wait()
        two = asyncio.create_task(self.service.get(9, 5))
        await asyncio.sleep(0)
        one.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await one
        release.set()
        self.assertEqual((await two).characters, CHARACTERS)
        self.fetcher.fetch_birthdays.assert_awaited_once()

    async def test_memory_eviction_preserves_disk_cache_and_hot_path_no_io(self):
        self.service.memory_days = 2
        for day in (1, 2, 3):
            await self.service.get(9, day)
        self.assertEqual(list(self.service._memory), ["09-02", "09-03"])
        await self.service.get(9, 1)
        self.assertEqual(self.fetcher.fetch_birthdays.await_count, 3)
        original = self.store.get_day
        self.store.get_day = lambda day: self.fail("hot path touched disk")
        for _ in range(1000):
            await self.service.get(9, 1)
        self.store.get_day = original
        self.assertEqual(self.store.stats()["days"], 3)

    async def test_force_refresh_replaces_successful_data(self):
        await self.service.get(9, 5)
        self.fetcher.fetch_birthdays.return_value = FetchResult(CHARACTERS[:1])
        await self.service.get(9, 5, force=True)
        self.assertEqual(self.fetcher.fetch_birthdays.await_count, 2)
        self.assertEqual(self.store.get_day("09-05").characters, CHARACTERS[:1])

    async def test_forced_failure_reports_fallback_even_when_cache_is_fresh(self):
        first = await self.service.get(9, 5)
        self.fetcher.fetch_birthdays.side_effect = ConnectionError("offline")
        fallback = await self.service.get(9, 5, force=True)
        self.assertTrue(fallback.fallback)
        self.assertEqual(fallback.fetched_at, first.fetched_at)
        self.assertFalse(self.store.get_day("09-05").fallback)

    async def test_close_cancels_fetches_and_closes_http(self):
        started = asyncio.Event()

        async def fetch(*args):
            started.set()
            await asyncio.Event().wait()

        self.fetcher.fetch_birthdays.side_effect = fetch
        task = asyncio.create_task(self.service.get(9, 5))
        await started.wait()
        await self.service.close()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertFalse(self.service._inflight)
        self.fetcher.close.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
