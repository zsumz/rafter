"""Execute the real runtime helper against isolated, observable package tools."""

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
HELPER = ROOT / ".github/actions/setup-invariant-runtime/install-packages.sh"
PACKAGES = ["bash=5.2.21-2ubuntu4", "perl=5.38.2-3.2ubuntu0.6"]

# No real sudo, APT, dpkg, or sleep can run. A subprocess still executes the
# production shell control flow and forwards its actual argument boundaries.
TOOL = r'''
import json
import os
from pathlib import Path
import sys
import time

root = Path(os.environ["RUNTIME_FIXTURE"])
scenario = json.loads((root / "scenario.json").read_text())
tool = Path(sys.argv[0]).name
args = sys.argv[1:]
row = {"tool": tool, "args": args, "pid": os.getpid(),
       "frontend": os.environ.get("DEBIAN_FRONTEND")}
if tool == "apt-get":
    phase = "update" if "update" in args else "download" if "--download-only" in args else "install"
    row["phase"] = phase
with (root / "calls.jsonl").open("a") as log:
    log.write(json.dumps(row) + "\n")
if tool == "sudo":
    os.execvpe(args[0], args, os.environ)
if tool == "apt-get":
    counter = root / (phase + ".count")
    count = int(counter.read_text()) if counter.exists() else 0
    counter.write_text(str(count + 1))
    codes = scenario.get(phase, [0])
    code = codes[min(count, len(codes) - 1)]
    if phase == "update" and "--error-on=any" not in args:
        code = 0  # APT can otherwise accept a partial index update.
    if phase == "download":
        archive = next(arg.split("=", 1)[1] for arg in args if arg.startswith("Dir::Cache::archives="))
        partial = Path(archive) / "retained.deb"
        if count:
            assert partial.read_text() == "verified partial download"
        partial.write_text("verified partial download")
    if scenario.get("block") == phase:
        (root / "blocked").write_text(str(os.getpid()))
        while True:
            time.sleep(1)
    if code:
        print(scenario.get("diagnostic", "503 Service Unavailable"), file=sys.stderr)
    sys.exit(code)
if tool == "sleep":
    if scenario.get("block") == "sleep":
        (root / "blocked").write_text(str(os.getpid()))
        while True:
            time.sleep(1)
    sys.exit(scenario.get("sleep_exit", 0))
if tool == "dpkg-query":
    if scenario.get("query_exit"):
        sys.exit(scenario["query_exit"])
    pins = dict(package.split("=", 1) for package in scenario["pins"])
    print(scenario.get("installed", pins[args[-1]]))
    sys.exit(0)
raise AssertionError(tool)
'''


class Fixture:
    def __init__(self, **scenario):
        self.temporary = tempfile.TemporaryDirectory(prefix="rafter-runtime-retrieval-")
        self.root = Path(self.temporary.name)
        self.state = self.root / "runtime state"
        self.state.mkdir()
        (self.state / "lists").mkdir()
        (self.state / "ubuntu.sources").write_text("fixture signed-source placeholder\n")
        commands = self.root / "bin"
        commands.mkdir()
        for name in ["sudo", "apt-get", "sleep", "dpkg-query"]:
            path = commands / name
            path.write_text(f"#!{sys.executable}\n" + TOOL)
            path.chmod(0o755)
        scenario.setdefault("pins", PACKAGES)
        (self.root / "scenario.json").write_text(json.dumps(scenario))
        self.pins = scenario["pins"]
        self.env = dict(os.environ, RUNTIME_FIXTURE=str(self.root),
                        PATH=str(commands) + os.pathsep + os.environ["PATH"])
        for name in ["BASH_ENV", "ENV", "CDPATH"]:
            self.env.pop(name, None)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.temporary.cleanup()

    def command(self, packages=None):
        return ["bash", str(HELPER), str(self.state), *(self.pins if packages is None else packages)]

    def run(self, packages=None):
        return subprocess.run(self.command(packages), env=self.env, capture_output=True,
                              text=True, timeout=10, check=False)

    def calls(self, tool=None):
        path = self.root / "calls.jsonl"
        rows = [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
        return [row for row in rows if tool is None or row["tool"] == tool]

    def cancel_when_blocked(self):
        process = subprocess.Popen(self.command(), env=self.env, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True, start_new_session=True)
        try:
            deadline = time.monotonic() + 5
            while not (self.root / "blocked").exists():
                if process.poll() is not None:
                    raise AssertionError(process.communicate())
                if time.monotonic() >= deadline:
                    raise AssertionError("fixture never reached the cancellation boundary")
                time.sleep(0.01)
            os.killpg(process.pid, signal.SIGTERM)
            stdout, stderr = process.communicate(timeout=5)
            return subprocess.CompletedProcess(process.args, process.returncode, stdout, stderr)
        finally:
            # Test failures must not leave a fake package process or sleep alive.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.communicate(timeout=5)
