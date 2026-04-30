"""Portable timed input polling for analysis review points."""

from __future__ import annotations

import os
import queue
import select
import sys
import time
from threading import Event, Lock, Thread
from typing import TextIO


class UserInteractionChannel:
    """Collect user input lines on a background thread.

    Uses select() with a 100ms timeout instead of a plain blocking readline()
    so that stop() can actually shut the thread down. A blocked readline cannot
    be interrupted from outside the thread; a select+stop_event pattern can.
    Crucial when the dashboard hands the terminal to a native TUI via /chat —
    a zombie reader thread on stdin would race the TUI for keystrokes and
    silently steal half the user's input.
    """

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

    def stop(self, *, join_timeout: float = 1.0) -> None:
        """Request that the background reader stop and wait for it to exit.

        With the select-based read loop, stop() can actually wait for the
        thread to drain its current 100ms tick and exit cleanly. The default
        1s timeout is generous enough that the thread is gone before the
        caller hands the terminal to anything else."""

        self._stop_event.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=join_timeout)

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
        # Resolve a real fd for select + os.read. Streams that wrap a file
        # descriptor (sys.stdin, os.pipe ends) expose it via fileno(); StringIO
        # and other in-memory streams fall back to a blocking readline.
        try:
            fileno = self._stream.fileno()
        except (AttributeError, OSError, ValueError):
            fileno = None

        if fileno is None:
            self._read_loop_fallback()
            return

        # Read directly from the OS fd to bypass Python's internal buffer —
        # select() only checks fd-level readiness, but readline() may have
        # already drained extra data into Python's TextIOWrapper buffer where
        # select can't see it. Build lines ourselves.
        buf = b""
        while not self._stop_event.is_set():
            try:
                ready, _, _ = select.select([fileno], [], [], 0.1)
            except (OSError, ValueError):
                return
            if not ready:
                continue
            try:
                chunk = os.read(fileno, 4096)
            except OSError:
                return
            if not chunk:
                return  # EOF
            buf += chunk
            while b"\n" in buf:
                line_bytes, _, buf = buf.partition(b"\n")
                self._lines.put(line_bytes.decode("utf-8", errors="replace").rstrip("\r"))

    def _read_loop_fallback(self) -> None:
        while not self._stop_event.is_set():
            line = self._stream.readline()
            if line == "":
                if self._stop_event.wait(0.05):
                    return
                continue
            self._lines.put(line.rstrip("\r\n"))


__all__ = ["UserInteractionChannel"]
