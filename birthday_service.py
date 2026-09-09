"""日期缓存、并发请求合并及断网回退，不依赖 AstrBot。"""

import asyncio
import logging
import time
from collections import OrderedDict
from dataclasses import replace

if __package__:
    from .storage import BirthdayEntry, BirthdayStore, date_key
else:
    from storage import BirthdayEntry, BirthdayStore, date_key

logger = logging.getLogger(__name__)


class BirthdayService:
    def __init__(self, store: BirthdayStore, fetcher, ttl_days: int = 30, memory_days: int = 8):
        self.store = store
        self.fetcher = fetcher
        self.ttl = ttl_days * 86400
        self.memory_days = memory_days
        self._memory: OrderedDict[str, BirthdayEntry] = OrderedDict()
        self._inflight: dict[str, asyncio.Task] = {}
        self._retry_after: dict[str, float] = {}
        self._fetch_lock = asyncio.Lock()
        self._closed = False

    def is_fresh(self, entry: BirthdayEntry) -> bool:
        ttl = self.ttl if entry.complete else 600
        if not entry.characters:
            ttl = min(ttl, 21600)
        return 0 <= time.time() - entry.fetched_at < ttl

    def _remember(self, key: str, entry: BirthdayEntry):
        self._memory[key] = entry
        self._memory.move_to_end(key)
        while len(self._memory) > self.memory_days:
            self._memory.popitem(last=False)

    async def get(self, month: int, day: int, force: bool = False) -> BirthdayEntry:
        key = date_key(month, day)
        if self._closed:
            raise ConnectionError("插件正在停止")
        cached = self._memory.get(key)
        if cached is not None and not force and self.is_fresh(cached):
            self._memory.move_to_end(key)
            return cached
        task = self._inflight.get(key)
        if task is None:
            if len(self._inflight) >= 8:
                if cached is not None:
                    return replace(cached, fallback=True)
                raise ConnectionError("查询队列已满，请稍后再试")
            task = asyncio.create_task(self._load(key, month, day, force))
            self._inflight[key] = task
            task.add_done_callback(lambda finished: self._finished(key, finished))
        # 一个查询被取消时，不取消其他查询和定时推送共用的抓取。
        return await asyncio.shield(task)

    def _finished(self, key: str, task: asyncio.Task):
        if self._inflight.get(key) is task:
            self._inflight.pop(key, None)
        if not task.cancelled():
            task.exception()  # 所有等待者取消后，也消费后台任务异常。

    async def _load(self, key: str, month: int, day: int, force: bool) -> BirthdayEntry:
        cached = self._memory.get(key)
        if cached is None:
            cached = await asyncio.to_thread(self.store.get_day, key)
        if cached is not None:
            self._remember(key, cached)
            if not force and self.is_fresh(cached):
                return cached
        if time.monotonic() < self._retry_after.get(key, 0):
            if cached is not None:
                return replace(cached, fallback=True)
            raise ConnectionError("上次抓取失败，10 分钟内暂缓重试")
        async with self._fetch_lock:
            try:
                result = await self.fetcher.fetch_birthdays(month, day)
            except ConnectionError:
                self._retry_after[key] = time.monotonic() + 600
                if cached is not None:
                    logger.warning("%s 更新失败，使用本地生日缓存", key)
                    return replace(cached, fallback=True)
                raise
            if not result.complete and cached is not None:
                # 不用不完整的新结果覆盖旧数据，也不延长旧数据的有效期。
                self._retry_after[key] = time.monotonic() + 600
                logger.warning("%s 抓取不完整，保留并使用本地生日缓存", key)
                return replace(cached, fallback=True)
            entry = BirthdayEntry(result.characters, time.time(), result.complete)
            await asyncio.to_thread(self.store.put_day, key, entry)
            self._remember(key, entry)
            self._retry_after.pop(key, None)
            return entry

    async def close(self):
        self._closed = True
        tasks = list(self._inflight.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._inflight.clear()
        await self.fetcher.close()
        self._memory.clear()
