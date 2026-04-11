"""Portable timed input polling for analysis review points."""

from __future__ import annotations

import queue
import sys
import time
from threading import Event, Lock, Thread
from typing import TextIO


class UserInteractionChannel:
    """Collect user input lines on a background thread for timed review windows."""

    def __init__(self, stream: TextIO | None = None) -> None:
        self._stream = stream or sys.stdin
        self._lines: queue.Queue[str] = queue.Queue()
        self._stop_event = Event()
        self._thread: Thread | None = None
        self._lock = Lock()

    def start(self) -> None:
        """Start the background reader if it is not already running."""

        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = Thread(target=self._read_loop, name="juvenal-analysis-input", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        """Request that the background reader stop."""

        self._stop_event.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=0.1)

    def poll(self, timeout: float) -> list[str]:
        """Collect every line entered during the bounded review window."""

        if timeout <= 0:
            return self._drain_lines()

        deadline = time.monotonic() + timeout
        lines: list[str] = []
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                lines.append(self._lines.get(timeout=remaining))
            except queue.Empty:
                break
        lines.extend(self._drain_lines())
        return lines

    def _drain_lines(self) -> list[str]:
        lines: list[str] = []
        while True:
            try:
                lines.append(self._lines.get_nowait())
            except queue.Empty:
                return lines

    def _read_loop(self) -> None:
        while not self._stop_event.is_set():
            line = self._stream.readline()
            if line == "":
                if self._stop_event.wait(0.05):
                    return
                continue
            self._lines.put(line.rstrip("\r\n"))


__all__ = ["UserInteractionChannel"]
