"""Teamsent smoke test: relay + host + guests over ptys.

usage: python3 -I scripts/teamsent-smoke.py target/debug/teamsent /tmp/ts-e2e
The work dir must be short (unix socket paths are limited to ~104 bytes).
Set TEAMSENT_RELAY=wss://… to test against a deployed relay instead of a local one.
"""
import json, os, pty, re, select, signal, struct, subprocess, sys, time, fcntl, termios

BIN, ROOT = sys.argv[1], sys.argv[2]
PORT = "17878"
env = {k: v for k, v in os.environ.items() if not k.startswith("HERDR")}
for d in ("XDG_CONFIG_HOME", "XDG_STATE_HOME", "XDG_DATA_HOME", "XDG_RUNTIME_DIR", "XDG_CACHE_HOME"):
    env[d] = os.path.join(ROOT, d.lower())
    os.makedirs(env[d], exist_ok=True)
env["TMPDIR"] = os.path.join(ROOT, "tmp") + "/"
os.makedirs(env["TMPDIR"], exist_ok=True)


def run(*args, timeout=10):
    r = subprocess.run([BIN, *args], env=env, capture_output=True, text=True, timeout=timeout)
    return r.returncode, r.stdout + r.stderr


procs = []


def bg(name, *args, stdin=None):
    log = open(os.path.join(ROOT, f"{name}.log"), "w")
    p = subprocess.Popen([BIN, *args], env=env, stdin=stdin, stdout=log, stderr=subprocess.STDOUT)
    procs.append(p)
    return p, os.path.join(ROOT, f"{name}.log")


def wait_log(path, pattern, timeout, start=0):
    end = time.time() + timeout
    while time.time() < end:
        m = re.search(pattern, open(path).read()[start:])
        if m:
            return m
        time.sleep(0.1)
    return None


def focused_pane():
    _, out = run("pane", "list")
    return next(p["pane_id"] for p in json.loads(out)["result"]["panes"] if p["focused"])


def pane_text(pane):
    return run("pane", "read", pane, "--source", "recent", "--lines", "80")[1]


class Guest:
    def __init__(self, link, name):
        self.pid, self.fd = pty.fork()
        if self.pid == 0:
            os.execve(BIN, [BIN, "join", link, "--name", name], env)
        fcntl.ioctl(self.fd, termios.TIOCSWINSZ, struct.pack("HHHH", 40, 120, 0, 0))
        self.closed = False

    def drain(self, seconds):
        out = b""
        end = time.time() + seconds
        while time.time() < end and not self.closed:
            r, _, _ = select.select([self.fd], [], [], 0.2)
            if r:
                try:
                    chunk = os.read(self.fd, 65536)
                except OSError:
                    chunk = b""
                if not chunk:
                    self.closed = True
                    break
                out += chunk
        return out

    def type(self, data):
        try:
            os.write(self.fd, data)
        except OSError:
            self.closed = True

    def exited(self, seconds):
        end = time.time() + seconds
        while time.time() < end:
            pid, _ = os.waitpid(self.pid, os.WNOHANG)
            if pid:
                self.pid = None
                return True
            self.drain(0.2)
        return False

    def kill(self):
        # Close the pty first: a child blocked flushing tty output never finishes exiting.
        try:
            os.close(self.fd)
        except OSError:
            pass
        if self.pid:
            try:
                os.kill(self.pid, signal.SIGKILL)
                os.waitpid(self.pid, 0)
            except OSError:
                pass


checks = {}
guests = []
try:
    relay = os.environ.get("TEAMSENT_RELAY")
    if not relay:
        relay = f"ws://127.0.0.1:{PORT}"
        _, relay_log = bg("relay", "relay", "--listen", f"127.0.0.1:{PORT}")
        assert wait_log(relay_log, r"listening", 5), "relay did not start"
    host, host_log = bg("host", "host", "--relay", relay, stdin=subprocess.PIPE)
    m = wait_log(host_log, r"teamsent join (\S+)", 15)
    assert m, "host printed no link:\n" + open(host_log).read()
    viewer_link = m.group(1)

    def host_cmd(line, expect, timeout=5):
        start = len(open(host_log).read())
        host.stdin.write((line + "\n").encode())
        host.stdin.flush()
        return wait_log(host_log, expect, timeout, start)

    pane = focused_pane()
    run("pane", "run", pane, "echo TEAMSENT_$((40+2))_MARK")
    time.sleep(1)

    # Bad token: refused before the client UI starts.
    code, out = run("join", re.sub(r"/\w+$", "/bad", viewer_link), timeout=10)
    checks["bad token refused"] = code != 0 and "unknown invite" in out

    # Viewer: sees everything, cannot type.
    viewer = Guest(viewer_link, "viewer1")
    guests.append(viewer)
    checks["viewer sees host pane"] = b"TEAMSENT_42_MARK" in viewer.drain(6)
    run("pane", "run", pane, "echo LIVE_$((5+5))_UPDATE")
    checks["viewer sees live update"] = b"LIVE_10_UPDATE" in viewer.drain(3)
    viewer.type(b"echo VIEWER_$((1+1))_INPUT\r")
    viewer.type(b"\x02c")  # prefix + c: try to create a tab
    viewer.drain(2)
    viewer.type(b"\x1b")  # leave the prefix/navigate mode the shortcut left us in
    viewer.drain(1)
    checks["viewer input blocked"] = "VIEWER_2_INPUT" not in pane_text(pane)
    checks["host focus unchanged"] = focused_pane() == pane
    checks["no tab created by viewer"] = run("tab", "list")[1].count('"tab_id"') == 1

    # Writer invite: input reaches the pane.
    m = host_cmd("/invite writer 10m", r"teamsent join (\S+)")
    assert m, "no writer link"
    writer = Guest(m.group(1), "writer1")
    guests.append(writer)
    writer.drain(4)
    writer.type(b"echo WRITER_$((3+4))_INPUT\r")
    writer.drain(2)
    checks["writer input reaches pane"] = "WRITER_7_INPUT" in pane_text(pane)

    # /grant: the viewer becomes a writer without reconnecting.
    checks["/grant reported"] = bool(host_cmd("/grant 1 writer", r"#1 viewer1 is now writer"))
    viewer.type(b"echo GRANTED_$((2+3))_INPUT\r")
    viewer.drain(2)
    checks["granted viewer can type"] = "GRANTED_5_INPUT" in pane_text(pane)

    # /revoke: the writer is kicked and its link stops working.
    checks["/revoke reported"] = bool(host_cmd("/revoke 2", r"invite \w+… revoked"))
    checks["revoked guest disconnected"] = writer.exited(8)
    code, out = run("join", m.group(1), timeout=10)
    checks["revoked link refused"] = code != 0 and "invite revoked" in out
finally:
    for g in guests:
        g.kill()
    for p in procs:
        p.send_signal(signal.SIGINT)
    time.sleep(0.5)
    for p in procs:
        p.kill()
    run("server", "stop")

print("--- host log ---")
print(open(os.path.join(ROOT, "host.log")).read())
for name, ok in checks.items():
    print(("PASS " if ok else "FAIL ") + name)
print("RESULT:", "PASS" if checks and all(checks.values()) else "FAIL")
