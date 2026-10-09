"""Teamsent smoke test: relay + host + guests over ptys.

usage: python3 -I scripts/teamsent-smoke.py target/debug/teamsent /tmp/ts-e2e
The work dir must be short (unix socket paths are limited to ~104 bytes).
Set TEAMSENT_RELAY=wss://… to test against a deployed relay instead of a local one.
"""
import json, os, pty, re, signal, struct, subprocess, sys, threading, time, fcntl, termios

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


def team_state():
    return run("team", "--once")[1]


def team_send(cmd):
    return run("team", "--send", json.dumps(cmd))


def pane_text(pane):
    return run("pane", "read", pane, "--source", "recent", "--lines", "80")[1]


class Guest:
    """A `teamsent join` in a pty, read continuously so it never blocks on output."""

    def __init__(self, link, name):
        self.pid, self.fd = pty.fork()
        if self.pid == 0:
            os.execve(BIN, [BIN, "join", link, "--name", name], env)
        fcntl.ioctl(self.fd, termios.TIOCSWINSZ, struct.pack("HHHH", 40, 120, 0, 0))
        self.closed = False
        self.buf = b""
        self.seen = 0
        self.lock = threading.Lock()
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        while True:
            try:
                chunk = os.read(self.fd, 65536)
            except OSError:
                chunk = b""
            if not chunk:
                self.closed = True
                return
            with self.lock:
                self.buf += chunk

    def drain(self, seconds):
        """Output produced since the last call, after waiting `seconds`."""
        time.sleep(seconds)
        with self.lock:
            out = self.buf[self.seen:]
            self.seen = len(self.buf)
        return out

    def wait_for(self, needle, seconds):
        """True once `needle` appears in output produced from now on."""
        start = self.seen
        end = time.time() + seconds
        while time.time() < end:
            with self.lock:
                if needle in self.buf[start:]:
                    self.seen = len(self.buf)
                    return True
            time.sleep(0.2)
        return False

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
            time.sleep(0.2)
        return False

    def kill(self):
        if self.pid:
            try:
                os.kill(self.pid, signal.SIGKILL)
                os.waitpid(self.pid, 0)
            except OSError:
                pass
        try:
            os.close(self.fd)
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

    checks["team window opened"] = bool(wait_log(host_log, r"Team window: pane", 10))
    pane = focused_pane()
    run("pane", "run", pane, "echo TEAMSENT_$((40+2))_MARK")
    time.sleep(1)

    # Bad token: refused before the client UI starts.
    code, out = run("join", re.sub(r"/\w+$", "/bad", viewer_link), timeout=10)
    checks["bad token refused"] = code != 0 and "unknown invite" in out

    # Viewer: sees everything, cannot type.
    viewer = Guest(viewer_link, "viewer1")
    guests.append(viewer)
    checks["viewer sees host pane"] = viewer.wait_for(b"TEAMSENT_42_MARK", 15)
    run("pane", "run", pane, "echo LIVE_$((5+5))_UPDATE")
    checks["viewer sees live update"] = viewer.wait_for(b"LIVE_10_UPDATE", 10)
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

    # /grant: the viewer becomes a writer without reconnecting, but writer1 holds the floor.
    checks["/grant reported"] = bool(host_cmd("/grant 1 writer", r"#1 viewer1 is now writer"))
    viewer.type(b"echo NOFLOOR_$((2+2))_INPUT\r")
    viewer.drain(2)
    checks["writer without floor blocked"] = "NOFLOOR_4_INPUT" not in pane_text(pane)
    checks["/drive gives floor"] = bool(host_cmd("/drive 1", r"#1 viewer1 has the floor"))
    viewer.type(b"echo GRANTED_$((2+3))_INPUT\r")
    viewer.drain(2)
    checks["driver can type"] = "GRANTED_5_INPUT" in pane_text(pane)
    writer.type(b"echo STOLEN_$((4+4))_INPUT\r")
    writer.drain(2)
    checks["other writer blocked while driving"] = "STOLEN_8_INPUT" not in pane_text(pane)
    checks["team shows driver"] = "floor: viewer1" in team_state()

    # Suggestions: viewers cannot suggest; a participant's suggestion waits for /y.
    code, out = run("say", viewer_link, "--name", "viewer2", "echo NOPE", timeout=15)
    checks["viewer cannot suggest"] = code != 0 and "viewers cannot suggest" in out
    m_part = host_cmd("/invite participant 10m", r"teamsent join (\S+)")
    assert m_part, "no participant link"
    code, out = run("say", m_part.group(1), "--name", "part1", "echo SUGGESTED_$((6+6))", timeout=15)
    sid = re.search(r"#(\d+) (?:sent to the host|queued)", out)
    checks["participant suggestion queued"] = code == 0 and bool(sid)
    checks["host sees suggestion"] = bool(wait_log(host_log, r"#\d+ from part1#\d+: echo SUGGESTED", 5))
    time.sleep(1)
    checks["not sent before approval"] = "SUGGESTED_12" not in pane_text(pane)
    if sid:
        host_cmd(f"/y {sid.group(1)} {pane}", r"accepted")
        checks["approved suggestion delivered"] = bool(wait_log(host_log, rf"#{sid.group(1)} delivered", 15))
        time.sleep(1)
        checks["suggestion ran in pane"] = "SUGGESTED_12" in pane_text(pane)
    code, out = run("say", m_part.group(1), "--name", "part1", "echo REJECTED_$((1+2))", timeout=15)
    sid2 = re.search(r"#(\d+) (?:sent to the host|queued)", out)
    if sid2:
        checks["/n rejects"] = bool(host_cmd(f"/n {sid2.group(1)}", r"rejected"))
        time.sleep(1)
        checks["rejected never sent"] = "REJECTED_3" not in pane_text(pane)
    checks["audit has suggestion events"] = bool(host_cmd("/log 50", r"suggestion_sent"))

    # Composer bar: Ctrl+] opens it, the draft is live in the Team window, Enter queues it.
    composer = Guest(m_part.group(1), "barguest")
    guests.append(composer)
    composer.drain(4)
    composer.type(b"\x1d")
    composer.type("fix the flaky test".encode())
    composer.drain(1.5)
    state = team_state()
    checks["team sees live draft"] = "barguest" in state and "typing: fix the flaky test" in state
    composer.type(b"\r")
    composer.drain(1.5)
    qline = next((l for l in team_state().splitlines() if l.startswith("queue") and "fix the flaky test" in l), "")
    checks["bar submit queued"] = "barguest#" in qline and "[pending]" in qline
    qid = re.search(r"queue #(\d+)", qline)
    if qid:
        code, out = team_send({"cmd": "edit", "id": int(qid.group(1)), "text": "echo EDITED_$((7+7))"})
        checks["team edit"] = code == 0
        code, out = team_send({"cmd": "approve", "id": int(qid.group(1)), "pane": pane})
        checks["team approve"] = code == 0 and "accepted" in out
        checks["team-approved delivered"] = bool(wait_log(host_log, rf"#{qid.group(1)} delivered", 15))
        time.sleep(1)
        checks["edited text ran"] = "EDITED_14" in pane_text(pane)

    # Auto mode (coshell-style): prompts queue without review; pause holds delivery.
    checks["/auto on"] = bool(host_cmd("/auto on", r"auto: guests"))
    checks["/pause"] = bool(host_cmd("/pause", r"delivery paused"))
    code, out = run("say", m_part.group(1), "--name", "part1", "echo AUTO_$((9+9))", timeout=15)
    checks["auto prompt queued"] = code == 0 and "queued, position 1" in out
    host_cmd("/p echo HOSTP_$((3+3))", r"queued, position 2")
    time.sleep(2)
    checks["paused: nothing delivered"] = "AUTO_18" not in pane_text(pane)
    state = team_state()
    checks["team shows auto+paused"] = "mode: auto paused" in state
    host_cmd("/resume", r"delivery resumed")
    checks["auto prompt delivered after resume"] = bool(wait_log(host_log, r"AUTO_18|#\d+ delivered", 15))
    time.sleep(3)
    text = pane_text(pane)
    checks["auto prompt ran with author"] = "part1: echo AUTO" in text or "AUTO_18" in text
    checks["host prompt ran"] = "HOSTP_6" in text
    host_cmd("/pause", r"delivery paused")
    run("say", m_part.group(1), "--name", "part1", "echo CLEARED_1", timeout=15)
    run("say", m_part.group(1), "--name", "part1", "echo CLEARED_2", timeout=15)
    checks["/clear"] = bool(host_cmd("/clear", r"queue cleared \(2 removed\)"))
    host_cmd("/resume", r"delivery resumed")
    host_cmd("/auto off", r"review:")
    time.sleep(2)
    checks["cleared never sent"] = "CLEARED_" not in pane_text(pane).replace("echo CLEARED", "")

    # Team window: role change and kick (what the right-click menu sends).
    gid = re.search(r"person #(\d+) barguest", team_state())
    if gid:
        code, out = team_send({"cmd": "role", "guest": int(gid.group(1)), "role": "viewer"})
        checks["team role change"] = code == 0 and "is now viewer" in out
        code, out = team_send({"cmd": "kick", "guest": int(gid.group(1))})
        checks["team kick"] = code == 0 and "barguest disconnected" in out
        checks["kicked guest gone"] = composer.exited(8)
        code, out = run("join", m_part.group(1), "--name", "barguest", timeout=10)
        checks["kicked link refused"] = code != 0 and "invite revoked" in out

    # /revoke: the writer is kicked and its link stops working.
    checks["/revoke reported"] = bool(host_cmd("/revoke 2", r"invite \w+… revoked"))
    checks["revoked guest disconnected"] = writer.exited(8)
    code, out = run("join", m.group(1), timeout=10)
    checks["revoked link refused"] = code != 0 and "invite revoked" in out

    # Stopping the host closes its Team window.
    panes_before = run("pane", "list")[1].count('"pane_id"')
    host.send_signal(signal.SIGINT)
    wait_log(host_log, r"Stopped sharing", 10)
    time.sleep(1)
    checks["team window closed on stop"] = run("pane", "list")[1].count('"pane_id"') == panes_before - 1
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
