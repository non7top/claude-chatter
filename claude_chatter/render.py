"""
Plain-text table output for the CLI.
"""
import os
import time
import shutil
import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

# (header, getter) or (header, getter, drop_rank): on a narrow terminal columns with a
# rank are dropped, highest rank first. The last column is never dropped.
Column = Union[Tuple[str, Callable[[Any], Any]], Tuple[str, Callable[[Any], Any], int]]

_MAX_CELL = 40
_MIN_LAST = 12
_GAP = "  "


def cell(value: Any) -> str:
    if value is None or value == "":
        return "-"
    return " ".join(str(value).split())


def _epoch(value: Any) -> Optional[float]:
    """Epoch seconds from seconds, milliseconds or an ISO string; None if unreadable."""
    if isinstance(value, bool) or value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return value / 1000 if value > 1e11 else float(value)
    try:
        return datetime.datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def fmt_time(value: Any) -> str:
    """Epoch seconds or milliseconds, or an ISO string, as 'YYYY-MM-DD HH:MM:SS'."""
    if value is None or value == "":
        return "-"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            return datetime.datetime.fromtimestamp(_epoch(value)).strftime("%Y-%m-%d %H:%M:%S")
        except (OverflowError, OSError, ValueError):
            return str(value)
    return str(value)[:19].replace("T", " ")


def fmt_ago(value: Any, now: Optional[float] = None) -> str:
    """Age as '5s ago', '3m ago', '2h ago' or '4d ago'; '-' when there is no readable time."""
    then = _epoch(value)
    if then is None:
        return "-"
    age = int((time.time() if now is None else now) - then)
    if age < 1:
        return "now"
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if age >= size:
            return f"{age // size}{unit} ago"
    return f"{age}s ago"


def home_to_tilde(path: str) -> str:
    home = os.path.expanduser("~").rstrip(os.sep)
    if home and path == home:
        return "~"
    if home and path.startswith(home + os.sep):
        return "~" + path[len(home):]
    return path


def _clip(text: str, width: int) -> str:
    if len(text) <= width:
        return text
    return text[:max(width - 3, 0)] + "..." if width > 3 else text[:width]


def render_table(
    columns: Sequence[Column],
    rows: Sequence[Any],
    width: Optional[int] = None,
    shorten_last: Optional[Callable[[str], str]] = None,
) -> str:
    """
    Header plus one line per row, no wider than the terminal. The last column takes the
    remaining width. When the table is too wide: shorten_last is applied to the last
    column first, then ranked columns are dropped until the last column (up to 40 characters)
    fits whole, then the others are narrowed and the last column is clipped with '...'.
    """
    if width is None:
        width = shutil.get_terminal_size(fallback=(120, 24)).columns
    columns = list(columns)
    cells = [[cell(col[1](row)) for col in columns] for row in rows]
    headers = [col[0] for col in columns]
    ranks = [col[2] if len(col) > 2 else 0 for col in columns]
    keep = list(range(len(columns)))
    tail = keep[-1]

    def natural(i: int) -> int:
        return max([len(headers[i])] + [len(r[i]) for r in cells])

    def gaps(idx: List[int]) -> int:
        return len(_GAP) * (len(idx) - 1)

    if shorten_last and sum(min(natural(i), _MAX_CELL) for i in keep[:-1]) + gaps(keep) + natural(tail) > width:
        for r in cells:
            r[tail] = shorten_last(r[tail])

    def fits(idx: List[int]) -> bool:
        return sum(min(natural(i), _MAX_CELL) for i in idx[:-1]) + gaps(idx) + min(natural(tail), _MAX_CELL) <= width

    while not fits(keep):
        droppable = [i for i in keep[:-1] if ranks[i]]
        if not droppable:
            break
        keep.remove(max(droppable, key=lambda i: ranks[i]))

    widths = {i: min(natural(i), _MAX_CELL) for i in keep[:-1]}
    while widths and sum(widths.values()) + gaps(keep) + _MIN_LAST > width:
        widest = max(widths, key=lambda i: widths[i])
        if widths[widest] <= len(_GAP):
            break
        widths[widest] -= 1
    room = width - sum(widths.values()) - gaps(keep)
    widths[tail] = max(min(natural(tail), room), _MIN_LAST)

    def line(values: List[str]) -> str:
        return _GAP.join(_clip(values[i], widths[i]).ljust(widths[i]) for i in keep).rstrip()

    return "\n".join([line(headers)] + [line(r) for r in cells])


def sessions_table(peers: Dict[str, Dict[str, Any]], width: Optional[int] = None) -> str:
    if not peers:
        return "(no active sessions)"
    columns: List[Column] = [
        ("NAME", lambda p: p.get("name")),
        ("PID", lambda p: p.get("pid")),
        ("KIND", lambda p: p.get("kind"), 2),
        ("STATUS", lambda p: p.get("status")),
        ("SID", lambda p: str(p.get("sessionId") or "")[:4], 3),
        ("UPDATED", lambda p: fmt_ago(p.get("updatedAt")), 1),
        ("CWD", lambda p: p.get("cwd")),
    ]
    rows = sorted(peers.values(), key=lambda p: str(p.get("name")))
    return render_table(columns, rows, width, shorten_last=home_to_tilde)


def jobs_table(jobs: List[Dict[str, Any]], width: Optional[int] = None) -> str:
    if not jobs:
        return "(no jobs)"
    columns: List[Column] = [
        ("ID", lambda j: j.get("id")),
        ("NAME", lambda j: j.get("name")),
        ("STATE", lambda j: j.get("state")),
        ("RUNNING", lambda j: "yes" if j.get("running") else "no", 2),
        ("PID", lambda j: (j.get("live") or {}).get("pid"), 3),
        ("UPDATED", lambda j: fmt_ago(j.get("updatedAt")), 1),
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
        ("STATUS", lambda m: m.get("status"), 1),
        ("READ", lambda m: ("yes" if m.get("read") else "no") if inbound(m) else None, 2),
        ("ID", lambda m: m.get("msg_id"), 3),
        ("CONTENT", lambda m: m.get("content")),
    ]
    rows = [dict(m, msg_id=m.get("msg_id", k)) for k, m in messages.items()]
    return render_table(columns, rows, width)
