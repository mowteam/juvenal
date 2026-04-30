"""Tests for UserInteractionChannel."""

from __future__ import annotations

import io
import os
import time

from juvenal.dynamic.interaction import UserInteractionChannel


def test_channel_reads_lines_from_pipe():
    r_fd, w_fd = os.pipe()
    stream = os.fdopen(r_fd, "r")
    channel = UserInteractionChannel(stream=stream)
    channel.start()
    try:
        os.write(w_fd, b"first line\n")
        os.write(w_fd, b"second line\n")
        deadline = time.monotonic() + 1.0
        collected: list[str] = []
        while time.monotonic() < deadline and len(collected) < 2:
            collected.extend(channel.poll(0.1))
        assert collected == ["first line", "second line"]
    finally:
        channel.stop()
        os.close(w_fd)
        stream.close()


def test_stop_actually_terminates_reader_thread():
    """The reader thread must exit promptly on stop, not hang in a blocking
    readline. Without this, /chat handing the terminal to a native TUI leaves
    a zombie reader thread racing the TUI for stdin."""
    r_fd, w_fd = os.pipe()
    stream = os.fdopen(r_fd, "r")
    channel = UserInteractionChannel(stream=stream)
    channel.start()
    started = time.monotonic()
    channel.stop(join_timeout=2.0)
    elapsed = time.monotonic() - started
    assert channel._thread is None or not channel._thread.is_alive()
    assert elapsed < 2.0
    os.close(w_fd)
    stream.close()


def test_stringio_fallback_path_works():
    """Stream-like objects without a fileno (StringIO, mocks) fall back to a
    degraded blocking readline path. Verify it doesn't crash; tests that need
    deterministic stop semantics should use a real pipe."""
    stream = io.StringIO("alpha\nbeta\n")
    channel = UserInteractionChannel(stream=stream)
    channel.start()
    deadline = time.monotonic() + 0.5
    collected: list[str] = []
    while time.monotonic() < deadline and len(collected) < 2:
        collected.extend(channel.poll(0.05))
    channel.stop(join_timeout=0.5)
    assert collected == ["alpha", "beta"]


def test_poll_timeout_returns_empty_when_no_input():
    r_fd, w_fd = os.pipe()
    stream = os.fdopen(r_fd, "r")
    channel = UserInteractionChannel(stream=stream)
    channel.start()
    try:
        started = time.monotonic()
        result = channel.poll(0.1)
        elapsed = time.monotonic() - started
        assert result == []
        assert elapsed >= 0.05
    finally:
        channel.stop()
        os.close(w_fd)
        stream.close()
