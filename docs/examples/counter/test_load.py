"""Exercise real socket deadlines, redirects, uncertain writes, and retained history."""
import asyncio
import hashlib
import json
from pathlib import Path
import tempfile
import time
import unittest

import load


class CounterLoad(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.requests = []
        self.connections = set()
        self.handlers = set()
        self.reply = self.normal_reply
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)

    async def asyncTearDown(self):
        self.server.close()
        for writer in tuple(self.connections):
            writer.close()
        for task in tuple(self.handlers):
            task.cancel()
        await asyncio.gather(*self.handlers, return_exceptions=True)
        await self.server.wait_closed()

    async def handle(self, reader, writer):
        task = asyncio.current_task()
        self.handlers.add(task)
        self.connections.add(writer)
        try:
            request = await reader.readuntil(b"\r\n\r\n")
            self.requests.append(request.split(b"\r\n", 1)[0])
            await self.reply(request, writer)
        finally:
            writer.close()
            self.connections.discard(writer)
            self.handlers.discard(task)

    async def normal_reply(self, request, writer):
        body = b'{"node":1,"ready":true}' if b"/metrics " in request else b'{"value":1}'
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)
        await writer.drain()

    def args(self, **changes):
        args = load.arguments(["--node", "1", "--output", str(Path(self.temporary.name) / "run"),
                               "--seconds", "2",
                               "--max-operations", "2", "--read-every", "2", "--pause-ms", "0"])
        args.http_base = self.port - 1
        for name, value in changes.items():
            setattr(args, name, value)
        return args

    async def test_history_pairs_every_invocation_and_binds_exact_bytes(self):
        args = self.args()
        self.assertEqual(await load.run(args), {"write_acknowledged": 1, "read_succeeded": 1})
        raw = (args.output / "history.jsonl").read_bytes()
        events = [json.loads(line) for line in raw.splitlines()]
        self.assertEqual([(e["event"], e["id"]) for e in events],
                         [("invoke", 1), ("complete", 1), ("invoke", 2), ("complete", 2)])
        summary = json.loads((args.output / "summary.json").read_text())
        self.assertEqual(summary["status"], "completed")
        self.assertEqual(summary["qualification"], "not_evaluated")
        self.assertEqual(summary["history_sha256"], hashlib.sha256(raw).hexdigest())
        self.assertEqual(summary["history_bytes"], len(raw))
        self.assertEqual(summary["unresolved_invocations"], 0)
        with self.assertRaises(FileExistsError):
            await load.run(args)
        self.assertEqual((args.output / "history.jsonl").read_bytes(), raw)

    async def test_whole_exchange_deadline_stops_a_dripping_response_without_retry(self):
        async def drip(request, writer):
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n")
            for _ in range(100):
                writer.write(b" ")
                await writer.drain()
                await asyncio.sleep(0.03)
        self.reply = drip
        start = time.monotonic()
        result = await load.operation(self.port, True, 0.15)
        self.assertEqual(result["outcome"], "write_uncertain")
        self.assertIn("TimeoutError", result["error"])
        self.assertLess(time.monotonic() - start, 1)
        self.assertEqual(len(self.requests), 1)

    async def test_redirect_is_recorded_without_contacting_its_location(self):
        async def redirect(request, writer):
            writer.write(b"HTTP/1.1 307 Temporary Redirect\r\nLocation: http://127.0.0.1:1/add/1\r\n"
                         b"Content-Length: 0\r\n\r\n")
            await writer.drain()
        self.reply = redirect
        result = await load.operation(self.port, True, 1)
        self.assertEqual(result["outcome"], "write_refused")
        self.assertEqual(result["status"], 307)
        self.assertEqual(len(self.requests), 1)

    async def test_wrong_replica_preflight_preserves_failure_without_writing(self):
        async def wrong(request, writer):
            body = b'{"node":2,"ready":true}'
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)
            await writer.drain()
        self.reply = wrong
        args = self.args()
        with self.assertRaisesRegex(ValueError, "requested ready counter"):
            await load.run(args)
        summary = json.loads((args.output / "summary.json").read_text())
        self.assertEqual((summary["status"], summary["invoked"]), ("incomplete", 0))
        self.assertEqual(self.requests, [b"GET /metrics HTTP/1.1"])

    async def test_cancellation_preserves_an_unresolved_write_invocation(self):
        received = asyncio.Event()
        async def hold(request, writer):
            if b"/metrics " in request:
                await self.normal_reply(request, writer)
            else:
                received.set()
                await asyncio.Event().wait()
        self.reply = hold
        args = self.args()
        task = asyncio.create_task(load.run(args))
        await asyncio.wait_for(received.wait(), 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        summary = json.loads((args.output / "summary.json").read_text())
        self.assertEqual((summary["status"], summary["unresolved_invocations"]), ("incomplete", 1))
        self.assertIn("CancelledError", summary["error"])
        events = [json.loads(line) for line in (args.output / "history.jsonl").read_text().splitlines()]
        self.assertEqual([(e["event"], e["kind"]) for e in events], [("invoke", "write")])
        self.assertEqual(len(self.requests), 2)

    async def test_oversized_response_is_refused_before_reading_body(self):
        async def oversized(request, writer):
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 999999999\r\n\r\n")
            await writer.drain()
            await asyncio.Event().wait()
        self.reply = oversized
        result = await load.operation(self.port, True, 1)
        self.assertEqual(result["outcome"], "write_uncertain")
        self.assertIn("body exceeds limit", result["error"])


class WriteFates(unittest.TestCase):
    def test_only_explicit_pre_append_refusal_clears_uncertainty(self):
        self.assertEqual(load.classify(True, 503, b'{"write_may_have_committed":false}')["outcome"],
                         "write_refused")
        for body in (b'{}', b'{"write_may_have_committed":true}',
                     b'{"write_may_have_committed":0}', b'{"write_may_have_committed":"false"}',
                     b'{"write_may_have_committed":true,"write_may_have_committed":false}', b'bad'):
            self.assertEqual(load.classify(True, 503, body)["outcome"], "write_uncertain")

    def test_malformed_success_cannot_acknowledge_a_write(self):
        for body in (b'{}', b'{"value":true}', b'{"value":-1}', b'[]', b'bad',
                     b'{"value":18446744073709551616}', b'{"value":1,"value":2}'):
            self.assertEqual(load.classify(True, 200, body)["outcome"], "write_uncertain")


if __name__ == "__main__":
    unittest.main()
