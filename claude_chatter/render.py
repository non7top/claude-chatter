"""
Plain-text table output for the CLI.
"""
import shutil
import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

Column = Tuple[str, Callable[[Any], Any]]

_MAX_CELL = 40
_MIN_LAST = 12
_GAP = "  "


def cell(value: Any) -> str:
    if value is None or value == "":
        return "-"
    return " ".join(str(value).split())


def fmt_time(value: Any) -> str:
    """Epoch seconds or milliseconds, or an ISO string, as 'YYYY-MM-DD HH:MM:SS'."""
    if value is None or value == "":
        return "-"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        seconds = value / 1000 if value > 1e11 else value
        try:
            return datetime.datetime.fromtimestamp(seconds).strftime("%Y-%m-%d %H:%M:%S")
        except (OverflowError, OSError, ValueError):
            return str(value)
    return str(value)[:19].replace("T", " ")


def _clip(text: str, width: int) -> str:
    if len(text) <= width:
        return text
    return text[:max(width - 3, 0)] + "..." if width > 3 else text[:width]


def render_table(columns: Sequence[Column], rows: Sequence[Any], width: Optional[int] = None) -> str:
    """Header plus one line per row; the last column takes the remaining terminal width."""
    if width is None:
        width = shutil.get_terminal_size(fallback=(120, 24)).columns
    cells = [[cell(get(row)) for _, get in columns] for row in rows]
    headers = [name for name, _ in columns]
    last = len(columns) - 1

    widths = []
    for i, name in enumerate(headers[:last]):
        widths.append(min(max([len(name)] + [len(r[i]) for r in cells]), _MAX_CELL))
    room = width - sum(widths) - len(_GAP) * last
    widths.append(max(min(max([len(headers[last])] + [len(r[last]) for r in cells]), room), _MIN_LAST))

    def line(values: List[str]) -> str:
        parts = [_clip(v, w).ljust(w) for v, w in zip(values, widths)]
        return _GAP.join(parts).rstrip()

    return "\n".join([line(headers)] + [line(r) for r in cells])


def sessions_table(peers: Dict[str, Dict[str, Any]], width: Optional[int] = None) -> str:
    if not peers:
        return "(no active sessions)"
    columns: List[Column] = [
        ("NAME", lambda p: p.get("name")),
        ("PID", lambda p: p.get("pid")),
        ("KIND", lambda p: p.get("kind")),
        ("STATUS", lambda p: p.get("status")),
        ("SESSION", lambda p: str(p.get("sessionId") or "")[:8]),
        ("UPDATED", lambda p: fmt_time(p.get("updatedAt"))),
        ("CWD", lambda p: p.get("cwd")),
    ]
    rows = sorted(peers.values(), key=lambda p: str(p.get("name")))
    return render_table(columns, rows, width)


def jobs_table(jobs: List[Dict[str, Any]], width: Optional[int] = None) -> str:
    if not jobs:
        return "(no jobs)"
    columns: List[Column] = [
        ("ID", lambda j: j.get("id")),
        ("NAME", lambda j: j.get("name")),
        ("STATE", lambda j: j.get("state")),
        ("RUNNING", lambda j: "yes" if j.get("running") else "no"),
        ("PID", lambda j: (j.get("live") or {}).get("pid")),
        ("UPDATED", lambda j: fmt_time(j.get("updatedAt"))),
        ("DETAIL", lambda j: j.get("detail")),
    ]
    return render_table(columns, jobs, width)


def messages_table(messages: Dict[str, Dict[str, Any]], width: Optional[int] = None) -> str:
    """One line per stored message, in the order given."""
    if not messages:
        return "(no messages)"
    inbound = lambda m: m.get("direction") == "inbound"  # noqa: E731
    columns: List[Column] = [
        ("TIME", lambda m: fmt_time(m.get("timestamp"))),
        ("DIR", lambda m: "in" if inbound(m) else "out"),
        ("PEER", lambda m: m.get("sender") if inbound(m) else m.get("target_session")),
        ("STATUS", lambda m: m.get("status")),
        ("READ", lambda m: ("yes" if m.get("read") else "no") if inbound(m) else None),
        ("ID", lambda m: m.get("msg_id")),
        ("CONTENT", lambda m: m.get("content")),
    ]
    rows = [dict(m, msg_id=m.get("msg_id", k)) for k, m in messages.items()]
    return render_table(columns, rows, width)
