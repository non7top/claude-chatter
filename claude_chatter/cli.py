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
    from .standalone import run_standalone
else:
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from claude_chatter.client import ClaudeClient
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


def main():
    parser = argparse.ArgumentParser(description="Claude Code peer messaging protocol and client CLI tool")
    parser.add_argument("--socket", type=str, default=None, help="Path to bridge Unix socket")
    parser.add_argument("--name", type=str, default=None, help="Session name announced to Claude Code (default: derived from cwd)")
    parser.add_argument("--db", type=str, default=None, help="Path to SQLite database for message persistence")
    parser.add_argument("--project-path", type=str, default=None, help="Explicit project directory path")

    mode_group = parser.add_argument_group("Execution Modes")
    mode_group.add_argument("--standalone", "--daemon", action="store_true", help="Run as standalone daemon listener")
    mode_group.add_argument("--list", action="store_true", help="List all active Claude Code sessions")
    mode_group.add_argument("--jobs", action="store_true", help="List Claude Code background jobs from ~/.claude/jobs")
    mode_group.add_argument("--inbox", action="store_true", help="List unread inbound messages from persisted inbox")
    mode_group.add_argument("--history", action="store_true", help="List full persisted message history")
    mode_group.add_argument("--send", nargs=2, metavar=("SESSION", "MESSAGE"), help="Send message to target Claude Code session")
    mode_group.add_argument("--purge", action="store_true", help="Proactively scan and purge dead session files")

    args = parser.parse_args()

    if args.list:
        client = ClaudeClient(bridge_socket_path=args.socket, session_name=args.name, enable_storage=False, as_bridge=False, project_path=args.project_path)

        async def _cmd_list():
            peers = await client.verify_and_purge_sessions()
            print(json.dumps(client.redact_peers(peers), indent=2))
        asyncio.run(_cmd_list())
        sys.exit(0)

    elif args.jobs:
        from .jobs import list_jobs
        client = ClaudeClient(bridge_socket_path=args.socket, session_name=args.name, enable_storage=False, as_bridge=False, project_path=args.project_path)

        async def _cmd_jobs():
            peers = await client.verify_and_purge_sessions()
            print(json.dumps(list_jobs(live_sessions=peers), indent=2))
        asyncio.run(_cmd_jobs())
        sys.exit(0)

    elif args.inbox:
        target_name = ClaudeClient.resolve_session_name(args.name, project_path=args.project_path, db_path=args.db)
        if not target_name:
            print(json.dumps({}, indent=2))
            sys.exit(0)
        db_file = args.db or os.environ.get("CHATTER_DB_PATH") or os.path.join(os.path.expanduser("~"), ".claude", "chatter-messages.db")
        if not os.path.exists(db_file):
            print(json.dumps({}, indent=2))
            sys.exit(0)
        from .storage import MessageStore
        store = MessageStore(db_file, read_only=True)
        all_msgs = store.load_session_messages(target_name)
        unread = {
            k: v for k, v in all_msgs.items()
            if v.get("direction") == "inbound" and not v.get("read", False)
        }
        unread = dict(sorted(unread.items(), key=lambda kv: kv[1].get("timestamp", 0)))
        print(json.dumps(unread, indent=2))
        store.close()
        sys.exit(0)

    elif args.history:
        target_name = ClaudeClient.resolve_session_name(args.name, project_path=args.project_path, db_path=args.db)
        if not target_name:
            print(json.dumps({}, indent=2))
            sys.exit(0)
        db_file = args.db or os.environ.get("CHATTER_DB_PATH") or os.path.join(os.path.expanduser("~"), ".claude", "chatter-messages.db")
        if not os.path.exists(db_file):
            print(json.dumps({}, indent=2))
            sys.exit(0)
        from .storage import MessageStore
        store = MessageStore(db_file, read_only=True)
        all_msgs = store.load_session_messages(target_name)
        all_msgs = dict(sorted(all_msgs.items(), key=lambda kv: kv[1].get("timestamp", 0)))
        print(json.dumps(all_msgs, indent=2))
        store.close()
        sys.exit(0)

    elif args.purge:
        client = ClaudeClient(bridge_socket_path=args.socket, session_name=args.name, enable_storage=False, as_bridge=False, project_path=args.project_path)

        async def _cmd_purge():
            peers = await client.verify_and_purge_sessions()
            print(f"Purge scan completed. Active sessions remaining: {len(peers)}")
        asyncio.run(_cmd_purge())
        sys.exit(0)

    elif args.send:
        client = ClaudeClient(bridge_socket_path=args.socket, session_name=args.name, enable_storage=False, as_bridge=False, project_path=args.project_path)
        target_session, message_text = args.send[0], args.send[1]

        async def _cmd_send():
            res = await client.send_message(
                target_session=target_session,
                message_content=message_text
            )
            print(json.dumps(res, indent=2))
            if not res.get("success"):
                sys.exit(1)
        asyncio.run(_cmd_send())
        sys.exit(0)

    elif args.standalone:
        client = ClaudeClient(bridge_socket_path=args.socket, session_name=args.name, db_path=args.db, as_bridge=True, project_path=args.project_path)
        try:
            asyncio.run(run_standalone(client))
        except (KeyboardInterrupt, SystemExit):
            logger.info("Shutting down standalone daemon gracefully.")
        except Exception as e:
            logger.error(f"Fatal standalone server error: {e}")
            sys.exit(0)

    else:
        parser.print_help()
        sys.exit(0)


if __name__ == "__main__":
    main()
