"""Asyncio helpers shared by every long-running loop: an interruptible sleep and a set of
tracked, eagerly started tasks."""

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any


async def pause(seconds: float, stop: asyncio.Event, *wake: asyncio.Event) -> bool:
    """Sleep up to `seconds` (negative: not at all), or until `stop` / any `wake` is set.
    True if stopping. A `wake` is left set: its owner clears it."""
    waiters = [asyncio.ensure_future(stop.wait())]
    waiters += [asyncio.ensure_future(w.wait()) for w in wake]
    try:
        await asyncio.wait(waiters, timeout=seconds, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for w in waiters:
            w.cancel()
    return stop.is_set()


class TaskSet:
    """Tasks started eagerly and tracked until they finish; `on_done` sees each one after it
    has left the set."""

    def __init__(self, on_done: Callable[[asyncio.Task], None] | None = None) -> None:
        self._tasks: set[asyncio.Task] = set()
        self._on_done = on_done

    def spawn(self, coro: Coroutine[Any, Any, Any], name: str) -> asyncio.Task:
        # eager: the coroutine is inside its own try/finally before anyone can cancel it, so
        # a task cancelled before its first step still cleans up
        task = asyncio.Task(coro, loop=asyncio.get_running_loop(), name=name, eager_start=True)
        if task.done():
            self._done(task)
            return task
        self._tasks.add(task)
        task.add_done_callback(self._done)
        return task

    def _done(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        if self._on_done is not None:
            self._on_done(task)

    def __len__(self) -> int:
        return len(self._tasks)

    async def drain(self) -> None:
        """Wait until every tracked task (and the tasks they start) has finished."""
        while self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)
            await asyncio.sleep(0)  # gather of done tasks never yields: let done callbacks run

    async def aclose(self) -> None:
        """Cancel every tracked task, then drain."""
        for task in list(self._tasks):
            task.cancel()
        await self.drain()
