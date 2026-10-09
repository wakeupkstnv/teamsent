# Working together with AI coding agents

How several people drive one agent (or several) without stepping on each other,
independent of the tool: Claude Code, Codex, OpenCode, Cursor CLI, …

## The problem

- An agent TUI has **one input box**. Two people typing into it interleave keystrokes.
- The agent is often **busy** (working) or **asking** (blocked on a permission prompt).
  Text typed then is lost, queued by the tool in its own way, or answers the wrong question.
- Ideas compete: two people want the agent to do different things *now*.
- Every tool has its own UI, so tool-specific integrations do not scale.

## Principle: work at the prompt level, not the keystroke level

Teamsent never needs to understand a tool's UI. herdr already gives us, for every
supported agent:

| herdr capability | used for |
|---|---|
| agent state: `idle` / `working` / `blocked` / `done` | when it is safe to send, when to hold |
| `agent prompt <pane> <text> --wait` | submit a whole prompt, wait for the turn to end |
| `agent read` | show what the agent is asking when blocked |
| `worktree.create`, `agent start` | parallel branches with the same or another agent |
| notifications | tell everyone what happened |

So collaboration is built from four layers on top of those primitives.

## 1. Queue (done, MVP)

```
guest: teamsent say <link> "add tests for the parser"
host:  #3 from kolya#2: add tests for the parser     /y 3   /n 3
       → delivered when the agent is idle; held while it asks a question
```

- Every suggestion has an author, a status (`pending → approved → sending → sent`
  or `rejected/failed`) and an audit entry.
- Delivery is sequential: the next item goes out only after the previous turn ended,
  so prompts never pile up inside the tool.
- Plain shell panes (no agent detected) get the text as a command line.

Next steps:
- **Auto-approve policy**: `/auto writer` — writers' suggestions skip approval.
- **Edit / merge before sending**: `/e 3` (edit text), `/merge 3 4` (one prompt from two ideas).
- **Reorder and vote**: `/up 4`; guests `teamsent vote 4` — host sees priority.
- **Per-pane queues**: one queue per agent pane when several agents run.

## 2. Floor: turn-taking for direct typing

Writers type straight into panes. To avoid interleaving:

- One person holds the **floor** for an agent pane. Only their keystrokes reach it;
  others' keystrokes into that pane are dropped (the host filter already sees which
  guest sent each `ClientShellPaneInput`, so this is a filter rule, not a UI change).
- Take the floor explicitly (`/floor 2`, later a keybinding) or implicitly by typing
  into a free pane. Released after N seconds idle or when the prompt is submitted.
- Everyone sees "kolya is typing in w1:p1" (notification or status line).
- Everyone else can still use the queue while the floor is taken.

## 3. Blocked state: who answers the agent's questions

When an agent asks "allow `rm -rf build`? (y/n)":

- Everyone gets a notification with the question (`agent read` of the last lines).
- Only the host answers by default (N9: subscriptions are the host's; the host owns risk).
  Policy option: writers may answer non-destructive prompts.
- Queue delivery is held until the question is answered (already implemented).

## 4. Branches: parallel exploration

When two ideas conflict, do not argue — run both:

```
/branch 4        # suggestion #4 runs in a new worktree with its own agent
```

- `worktree.create` → new pane in a new tab → `agent start` with the same kind of
  agent as the main pane (or `--agent codex` to compare tools) → `agent prompt` with
  the suggestion plus a short handoff of the main session (goal, decisions, recent diff;
  D11).
- Guests watch the branch tab like any other; its queue is separate.
- `/diff 4` shows `git diff main...teamsent/4`; `/adopt 4` merges or opens a PR;
  `/drop 4` removes the worktree.
- Works for any agent because the isolation is at the git/worktree level.

## 5. Where people type (UI)

- **v1 (now)**: host commands in the `teamsent host` terminal; guests use
  `teamsent say` from any terminal; herdr notifications in everyone's UI.
- **v2**: a composer inside the herdr UI for everyone — a keybinding opens an input,
  the text is sent as a teamsent endpoint request that the host filter intercepts
  (it knows the author from the connection), so it never reaches herdr itself.
  Needs research into herdr's popup and plugin APIs.
- **v3**: a queue pane (plugin pane) that lists suggestions, votes and branches.

## Order

1. Queue ✅ → auto-approve + edit/merge
2. Floor (filter rule + notifications)
3. Blocked-state broadcast
4. Branches via worktrees
5. In-UI composer
