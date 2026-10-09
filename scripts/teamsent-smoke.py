"""Teamsent smoke test: relay + host + view-only guest over a pty.

usage: python3 -I teamsent-smoke.py <herdr-binary> <short-work-dir>
The work dir must be short (unix socket paths are limited to ~104 bytes).
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


def bg(name, *args):
    log = open(os.path.join(ROOT, f"{name}.log"), "w")
    p = subprocess.Popen([BIN, *args], env=env, stdout=log, stderr=subprocess.STDOUT)
    procs.append(p)
    return os.path.join(ROOT, f"{name}.log")


def wait_log(path, pattern, timeout):
    end = time.time() + timeout
    while time.time() < end:
        m = re.search(pattern, open(path).read())
        if m:
            return m
        time.sleep(0.1)
    return None


def focused_pane():
    _, out = run("pane", "list")
    return next(p["pane_id"] for p in json.loads(out)["result"]["panes"] if p["focused"])


checks = {}
guest_pid = None
try:
    relay_log = bg("relay", "relay", "--listen", f"127.0.0.1:{PORT}")
    assert wait_log(relay_log, r"listening", 5), "relay did not start"
    host_log = bg("host", "host", "--relay", f"ws://127.0.0.1:{PORT}")
    m = wait_log(host_log, r"teamsent join (\S+)", 15)
    assert m, "host printed no link:\n" + open(host_log).read()
    link = m.group(1)

    pane = focused_pane()
    run("pane", "run", pane, "echo TEAMSENT_$((40+2))_MARK")
    time.sleep(1)

    guest_pid, fd = pty.fork()
    if guest_pid == 0:
        os.execve(BIN, [BIN, "join", link], env)
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", 40, 120, 0, 0))

    def drain(seconds):
        out = b""
        end = time.time() + seconds
        while time.time() < end:
            r, _, _ = select.select([fd], [], [], 0.2)
            if r:
                try:
                    out += os.read(fd, 65536)
                except OSError:
                    break
        return out

    screen = drain(6)
    checks["guest sees host pane"] = b"TEAMSENT_42_MARK" in screen
    if not checks["guest sees host pane"]:
        print("guest output tail:", screen[-1500:].decode(errors="replace"))

    run("pane", "run", pane, "echo LIVE_$((5+5))_UPDATE")
    checks["guest sees live update"] = b"LIVE_10_UPDATE" in drain(3)

    try:
        os.write(fd, b"echo GUEST_$((1+1))_INPUT\r")
        os.write(fd, b"\x02c")  # prefix + c: try to create a tab
    except OSError as e:
        print("guest pty closed early:", e)
    drain(2)
    _, text = run("pane", "read", pane, "--source", "recent", "--lines", "80")
    checks["guest input blocked"] = "GUEST_2_INPUT" not in text and "GUEST_$((1+1))_INPUT" not in text
    checks["host focus unchanged"] = focused_pane() == pane
    _, tabs = run("tab", "list")
    checks["no tab created by guest"] = tabs.count('"tab_id"') == 1
finally:
    if guest_pid:
        try:
            os.kill(guest_pid, signal.SIGKILL)
            os.waitpid(guest_pid, 0)
        except OSError:
            pass
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
