"""
CLI entry point for claude-chatter: argument parsing and mode dispatch.
"""
import os
import sys
import json
import asyncio
import logging
import argparse
from logging.handlers import RotatingFileHandler

if __package__:
    from .client import ClaudeClient
    from .render import jobs_table, messages_table, sessions_table
    from .standalone import run_standalone
else:
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from claude_chatter.client import ClaudeClient
    from claude_chatter.render import jobs_table, messages_table, sessions_table
    from claude_chatter.standalone import run_standalone

_LOG_FORMAT = "%(asctime)s [%(levelname)s] [pid=%(process)d] %(message)s"
_log_handlers = [logging.StreamHandler(sys.stderr)]
try:
    _log_dir = os.path.expanduser("~/.claude/logs")
    os.makedirs(_log_dir, exist_ok=True)
    _log_handlers.append(
        RotatingFileHandler(
            os.path.join(_log_dir, "claude-chatter.log"),
            maxBytes=5 * 1024 * 1024,
            backupCount=3
        )
    )
except OSError:
    pass

logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT, handlers=_log_handlers)
logger = logging.getLogger("claude_chatter")


def _add_common(parser, inherited):
    # Subparsers use SUPPRESS so a value given before the command is not overwritten by None.
    default = argparse.SUPPRESS if inherited else None
    parser.add_argument("--socket", type=str, default=default, help="Path to bridge Unix socket")
    parser.add_argument("--name", type=str, default=default, help="Session name announced to Claude Code (default: derived from cwd)")
    parser.add_argument("--db", type=str, default=default, help="Path to SQLite database for message persistence")
    parser.add_argument("--project-path", type=str, default=default, help="Explicit project directory path")


def build_parser():
    parser = argparse.ArgumentParser(description="Claude Code peer messaging protocol and client CLI tool")
    _add_common(parser, inherited=False)
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    def command(name, help_text, json_flag=False, aliases=()):
        p = sub.add_parser(name, help=help_text, description=help_text, aliases=list(aliases))
        _add_common(p, inherited=True)
        if json_flag:
            p.add_argument("--json", action="store_true", help="Print JSON instead of a table")
        return p

    command("list", "List all active Claude Code sessions", json_flag=True)
    command("jobs", "List Claude Code background jobs from ~/.claude/jobs", json_flag=True)
    command("inbox", "List unread inbound messages from persisted inbox", json_flag=True)
    command("history", "List full persisted message history", json_flag=True)
    send = command("send", "Send message to target Claude Code session", json_flag=True)
    send.add_argument("session", help="Target session name, PID, session id or partial name")
    send.add_argument("message", help="Message text")
    command("purge", "Proactively scan and purge dead session files")
    command("standalone", "Run as standalone daemon listener", aliases=["daemon"])
    return parser


def _new_client(args):
    return ClaudeClient(bridge_socket_path=args.socket, session_name=args.name, enable_storage=False, as_bridge=False, project_path=args.project_path)


def _emit(args, data, table):
    print(json.dumps(data, indent=2) if args.json else table(data))


def _cmd_list(args):
    client = _new_client(args)
    peers = client.redact_peers(asyncio.run(client.verify_and_purge_sessions()))
    _emit(args, peers, sessions_table)


def _cmd_jobs(args):
    from .jobs import list_jobs
    client = _new_client(args)
    peers = asyncio.run(client.verify_and_purge_sessions())
    _emit(args, list_jobs(live_sessions=peers), jobs_table)


def _stored_messages(args):
    target_name = ClaudeClient.resolve_session_name(args.name, project_path=args.project_path, db_path=args.db)
    if not target_name:
        return {}
    db_file = args.db or os.environ.get("CHATTER_DB_PATH") or os.path.join(os.path.expanduser("~"), ".claude", "chatter-messages.db")
    if not os.path.exists(db_file):
        return {}
    from .storage import MessageStore
    store = MessageStore(db_file, read_only=True)
    try:
        msgs = store.load_session_messages(target_name)
    finally:
        store.close()
    return dict(sorted(msgs.items(), key=lambda kv: kv[1].get("timestamp", 0)))


def _cmd_inbox(args):
    unread = {
        k: v for k, v in _stored_messages(args).items()
        if v.get("direction") == "inbound" and not v.get("read", False)
    }
    _emit(args, unread, messages_table)


def _cmd_history(args):
    _emit(args, _stored_messages(args), messages_table)


def _cmd_purge(args):
    client = _new_client(args)
    peers = asyncio.run(client.verify_and_purge_sessions())
    print(f"Purge scan completed. Active sessions remaining: {len(peers)}")


def _cmd_send(args):
    client = _new_client(args)
    res = asyncio.run(client.send_message(target_session=args.session, message_content=args.message))
    if args.json:
        print(json.dumps(res, indent=2))
    elif res.get("success"):
        print(f"Sent {res['msg_id']} to {res['target']} ({res['status']})")
    else:
        print(f"Send failed: {res.get('error')}", file=sys.stderr)
    if not res.get("success"):
        sys.exit(1)


def _cmd_standalone(args):
    client = ClaudeClient(bridge_socket_path=args.socket, session_name=args.name, db_path=args.db, as_bridge=True, project_path=args.project_path)
    try:
        asyncio.run(run_standalone(client))
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down standalone daemon gracefully.")
    except Exception as e:
        logger.error(f"Fatal standalone server error: {e}")
        sys.exit(0)


_COMMANDS = {
    "list": _cmd_list,
    "jobs": _cmd_jobs,
    "inbox": _cmd_inbox,
    "history": _cmd_history,
    "send": _cmd_send,
    "purge": _cmd_purge,
    "standalone": _cmd_standalone,
    "daemon": _cmd_standalone,
}


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    handler = _COMMANDS.get(args.command)
    if handler is None:
        parser.print_help()
        return
    handler(args)


if __name__ == "__main__":
    main()
