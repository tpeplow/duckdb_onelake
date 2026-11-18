"""Separate-process, loopback-only deterministic Arrow benchmark fixture server."""

import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import time
from urllib.parse import unquote

SCENARIOS = ("balanced", "skewed", "fast", "stalled", "early", "one")


def load_manifest(directory):
    directory = directory.resolve()
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    streams = {}
    for entry in manifest["streams"]:
        stream_id = entry["id"]
        path = (directory / entry["file"]).resolve()
        if path.parent != directory or not path.is_file() or stream_id in streams:
            raise ValueError("Invalid fixture manifest")
        ends = entry["batch_ends"]
        if (
            not isinstance(stream_id, str)
            or not stream_id.isdecimal()
            or not isinstance(ends, list)
            or not ends
            or any(type(end) is not int for end in ends)
            or ends != sorted(set(ends))
            or ends[0] <= 0
            or ends[-1] > path.stat().st_size
        ):
            raise ValueError("Invalid fixture boundaries")
        streams[stream_id] = {"path": path, "batch_ends": ends}
    if set(streams) != {str(index) for index in range(8)}:
        raise ValueError("Expected eight fixture streams")
    return streams


def stream_delay(scenario, stream_id):
    if scenario == "skewed":
        return 0.01 if stream_id == "0" else 0
    return 0.002 if scenario in ("balanced", "early", "one") else 0


def make_handler(streams, scenario):
    active_streams = {"0": streams["0"]} if scenario == "one" else streams

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_):
            pass

        def respond(self, status, body=b"", content_type="application/json"):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            if self.path != "/read":
                self.respond(404)
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                self.respond(400)
                return
            if not 0 <= length <= 65536:
                self.respond(400)
                return
            self.rfile.read(length)
            body = json.dumps({"streams": [{"streamId": stream_id} for stream_id in active_streams]}).encode()
            self.respond(200, body)

        def do_GET(self):
            if self.path == "/health":
                self.respond(200, b"{}")
                return
            if not self.path.startswith("/readStream/"):
                self.respond(404)
                return
            stream_id = unquote(self.path[len("/readStream/") :])
            if stream_id not in active_streams:
                self.respond(404)
                return
            entry = active_streams[stream_id]
            size = entry["path"].stat().st_size
            self.send_response(200)
            self.send_header("Content-Type", "application/vnd.apache.arrow.stream")
            self.send_header("Content-Length", str(size))
            self.end_headers()
            delay = stream_delay(scenario, stream_id)
            boundaries = [*entry["batch_ends"]]
            if boundaries[-1] != size:
                boundaries.append(size)
            try:
                with entry["path"].open("rb") as source:
                    position = 0
                    for end in boundaries:
                        if delay:
                            time.sleep(delay)
                        while position < end:
                            data = source.read(min(65536, end - position))
                            if not data:
                                raise ValueError("Truncated fixture")
                            self.wfile.write(data)
                            position += len(data)
                        self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                pass  # Early-stop clients intentionally abandon response bodies.

    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixtures", type=Path, required=True)
    parser.add_argument("--scenario", choices=SCENARIOS, required=True)
    args = parser.parse_args()
    streams = load_manifest(args.fixtures)
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(streams, args.scenario))
    server.daemon_threads = True
    print("BENCH_SERVER", flush=True)
    print(json.dumps({"url": f"http://127.0.0.1:{server.server_port}"}), flush=True)
    print("BENCH_READY", flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
