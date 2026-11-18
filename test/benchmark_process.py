"""Standard-library child measurements; never retain stderr/service error bodies."""

import ctypes
import math
import os
import subprocess
import threading
import time


class BenchmarkFailure(RuntimeError):
    """Only fixed, non-sensitive failure descriptions may be supplied."""


class ProcessCounters:
    def __init__(self, process):
        self.process = process
        self.available = os.name == "nt"
        if not self.available:
            return
        from ctypes import wintypes

        class Memory(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                *[
                    (name, ctypes.c_size_t)
                    for name in (
                        "PeakWorkingSetSize",
                        "WorkingSetSize",
                        "QuotaPeakPagedPoolUsage",
                        "QuotaPagedPoolUsage",
                        "QuotaPeakNonPagedPoolUsage",
                        "QuotaNonPagedPoolUsage",
                        "PagefileUsage",
                        "PeakPagefileUsage",
                        "PrivateUsage",
                    )
                ],
            ]

        self.memory_type = Memory
        self.kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        self.psapi = ctypes.WinDLL("psapi", use_last_error=True)
        self.psapi.GetProcessMemoryInfo.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(Memory),
            wintypes.DWORD,
        ]
        self.psapi.GetProcessMemoryInfo.restype = wintypes.BOOL
        self.kernel.GetProcessTimes.argtypes = [
            wintypes.HANDLE,
            *[ctypes.POINTER(wintypes.FILETIME)] * 4,
        ]
        self.kernel.GetProcessTimes.restype = wintypes.BOOL
        self.filetime = wintypes.FILETIME

    def sample(self):
        if not self.available:
            return None
        memory = self.memory_type()
        memory.cb = ctypes.sizeof(memory)
        if not self.psapi.GetProcessMemoryInfo(int(self.process._handle), ctypes.byref(memory), memory.cb):
            return None
        return {
            "working_set_bytes": memory.WorkingSetSize,
            "private_bytes": memory.PrivateUsage,
            "process_peak_working_set_bytes": memory.PeakWorkingSetSize,
            "process_cpu_seconds": self.cpu_seconds(),
        }

    def cpu_seconds(self):
        if not self.available:
            return None
        times = [self.filetime() for _ in range(4)]
        if self.kernel.GetProcessTimes(int(self.process._handle), *[ctypes.byref(value) for value in times]):
            return sum((value.dwHighDateTime << 32) + value.dwLowDateTime for value in times[2:]) / 10_000_000
        return None


class MeasuredChild:
    """Drain both pipes concurrently and hold interactive clients for final sampling."""

    def __init__(self, command, *, cwd=None, env=None, timeout=900, interval=0.01):
        if not all(math.isfinite(value) and value > 0 for value in (timeout, interval)):
            raise BenchmarkFailure("invalid_measurement_interval")
        self.started = time.perf_counter()
        self.deadline = self.started + timeout
        self.interval = interval
        self.process = subprocess.Popen(
            command,
            cwd=cwd,
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        self.counters = ProcessCounters(self.process)
        self.condition = threading.Condition()
        self.lines = []
        self.markers = set()
        self.output_bytes = 0
        self.overflow = False
        self.stdout_done = False
        self.baseline = None
        self.last = None
        self.peaks = {}
        self.samples = 0
        self.missing_samples = 0
        self.threads = [
            threading.Thread(target=self._drain_stdout),
            threading.Thread(target=self._drain_stderr),
        ]
        for thread in self.threads:
            thread.start()

    def _drain_stdout(self):
        try:
            while True:
                # Bound a single line too; a child need not emit newlines.
                line = self.process.stdout.readline(65536)
                if not line:
                    break
                with self.condition:
                    self.output_bytes += len(line)
                    if self.output_bytes <= 8 * 1024 * 1024:
                        self.lines.append(line)
                        if line.startswith("BENCH_"):
                            self.markers.add(line.strip())
                    else:
                        self.overflow = True
                    self.condition.notify_all()
        finally:
            with self.condition:
                self.stdout_done = True
                self.condition.notify_all()

    def _drain_stderr(self):
        while self.process.stderr.read(65536):
            pass

    def send(self, commands):
        try:
            self.process.stdin.write(commands)
            self.process.stdin.flush()
        except (BrokenPipeError, OSError):
            raise BenchmarkFailure("child_input_closed") from None

    def sample(self):
        if self.process.poll() is not None:
            return None
        value = self.counters.sample()
        if value is None:
            self.missing_samples += 1
            return None
        self.samples += 1
        self.last = value
        for key in ("working_set_bytes", "private_bytes", "process_peak_working_set_bytes"):
            self.peaks[key] = max(self.peaks.get(key, 0), value[key])
        return value

    def start_measurement(self):
        self.peaks = {}
        self.samples = self.missing_samples = 0
        self.baseline = self.sample()

    def wait_marker(self, marker, *, measure=False):
        self._wait(lambda: marker in self.markers, measure=measure)

    def wait_output_line(self, token, *, measure=False):
        self._wait(
            lambda: any(token in line and line.endswith("\n") for line in self.lines),
            measure=measure,
        )

    def _wait(self, ready, *, measure):
        while True:
            if measure:
                self.sample()
            with self.condition:
                if self.overflow:
                    raise BenchmarkFailure("child_output_limit")
                if ready():
                    return
                if self.stdout_done:
                    raise BenchmarkFailure("child_exited_before_marker")
                remaining = self.deadline - time.perf_counter()
                if remaining <= 0:
                    raise BenchmarkFailure("child_timeout")
                self.condition.wait(min(self.interval, remaining))

    def wait_exit(self, *, measure=False):
        while self.process.poll() is None:
            if measure:
                self.sample()
            if time.perf_counter() >= self.deadline:
                raise BenchmarkFailure("child_timeout")
            time.sleep(self.interval)
        for thread in self.threads:
            thread.join(timeout=5)
        if any(thread.is_alive() for thread in self.threads):
            raise BenchmarkFailure("child_pipe_shutdown")
        if self.overflow:
            raise BenchmarkFailure("child_output_limit")
        if self.process.returncode:
            raise BenchmarkFailure("child_nonzero_exit")

    def resources(self):
        return {
            "provider": (
                "Windows GetProcessMemoryInfo/GetProcessTimes"
                if self.counters.available
                else "unavailable on this platform"
            ),
            "sample_interval_seconds": self.interval,
            "samples": self.samples,
            "missing_samples": self.missing_samples,
            "query_baseline": self.baseline,
            "query_final_alive_sample": self.last,
            "query_sampled_peak_working_set_bytes": self.peaks.get("working_set_bytes"),
            "query_sampled_peak_private_bytes": self.peaks.get("private_bytes"),
            "process_peak_working_set_bytes": self.peaks.get("process_peak_working_set_bytes"),
            "peak_scope": "process high-water includes startup/setup; query peaks are sampled",
        }

    @property
    def stdout(self):
        with self.condition:
            return "".join(self.lines)

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        for thread in self.threads:
            thread.join(timeout=5)
        for pipe in (self.process.stdin, self.process.stdout, self.process.stderr):
            try:
                pipe.close()
            except OSError:
                pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
