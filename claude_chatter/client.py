"""
Claude Code messaging client: session discovery (~/.claude/sessions),
Unix-domain-socket transport (auth + message framing), session identity
persistence, and the send/receive primitives used to communicate with
other Claude Code sessions.
"""
import os
import sys
import re
import json
import glob
import uuid
import stat
import time
import string
import sqlite3
import hashlib
import secrets
import asyncio
import inspect
import logging
from typing import Any, Callable, Dict, Optional, Set

from .wire import ClaudeWire
from .storage import MessageStore, ResponseStore
from .exceptions import AmbiguousSessionError, SessionNameInUseError, StorageError

logger = logging.getLogger("claude_chatter")


class ClaudeClient:
    def __init__(
        self,
        bridge_socket_path: Optional[str] = None,
        session_name: Optional[str] = None,
        on_inbound_message: Optional[Callable[[Dict[str, Any]], Any]] = None,
        db_path: Optional[str] = None,
        enable_storage: bool = True,
        session_id: Optional[str] = None,
        as_bridge: bool = True,
        read_only: bool = False,
        project_path: Optional[str] = None,
    ):
        """
        on_inbound_message(event: dict), if given, is invoked whenever a real
        inbound message frame is processed. Dispatched after persisting to
        memory and SQLite.
        """
        self.pid = os.getpid()
        self.home_dir = os.path.expanduser("~")
        self.sessions_dir = os.path.join(self.home_dir, ".claude", "sessions")
        self.on_inbound_message = on_inbound_message
        self.as_bridge = as_bridge
        self.read_only = read_only
        self.project_path = project_path or os.environ.get("CHATTER_PROJECT_PATH") or os.getcwd()
        self._background_tasks: Set[asyncio.Task] = set()

        env_name = os.environ.get("CHATTER_SESSION_NAME")
        self.session_name = session_name or env_name or self._derive_default_session_name()

        env_socket = os.environ.get("CHATTER_BRIDGE_SOCKET")
        if bridge_socket_path:
            self.bridge_socket_path = bridge_socket_path
        elif env_socket:
            self.bridge_socket_path = env_socket
        else:
            self.bridge_socket_path = self._get_default_socket_path(self.pid, self.session_name)

        if enable_storage:
            self.db_path = db_path or os.environ.get("CHATTER_DB_PATH") or os.path.join(self.home_dir, ".claude", "chatter-messages.db")
            self.store = MessageStore(self.db_path, read_only=self.read_only)
            if not self.read_only:
                self.store.prune()
            if session_id:
                self.session_id = session_id
            elif getattr(self, "_is_clashing", False):
                self.session_id = str(uuid.uuid4())
            else:
                existing_id = self.store.resolve_session_id(self.session_name)
                if existing_id:
                    self.session_id = existing_id
                else:
                    persisted_name = self.read_persisted_session_name(self.project_path)
                    if not session_name or session_name == persisted_name:
                        self.session_id = self._derive_session_id()
                    else:
                        self.session_id = str(uuid.uuid4())
            if not self.read_only:
                self.store.ensure_session(self.session_name, cwd_digest=self._cwd_digest(), session_id=self.session_id)
                if self.as_bridge and not getattr(self, "_is_clashing", False) and not session_name and not env_name:
                    self.persist_default_session_name(self.session_name)
                    self.persist_session_id(self.session_id)
            initial_messages = self.store.load_session_messages(self.session_id or self.session_name)
            self.response_store = ResponseStore(
                store=self.store,
                session_name=self.session_name,
                session_id=self.session_id,
                cwd_digest=self._cwd_digest(),
                initial=initial_messages
            )
        else:
            self.store = None
            self.session_id = session_id or self._derive_session_id()
            self.response_store = {}

        self.active_peers: Dict[str, Dict[str, Any]] = {}
        self.session_json_path: Optional[str] = None
        self.session_key_path: Optional[str] = None
        self.named_descriptor_path: Optional[str] = None

    @property
    def persistence_error(self) -> Optional[str]:
        if self.store and self.store.is_disabled():
            return self.store.last_error or "MessageStore is disabled"
        return None

    def _find_project_path(self) -> str:
        return getattr(self, "project_path", None) or os.environ.get("CHATTER_PROJECT_PATH") or os.getcwd()

    @classmethod
    def read_persisted_session_name(cls, project_path: Optional[str] = None) -> str:
        path = project_path or os.environ.get("CHATTER_PROJECT_PATH") or os.getcwd()
        digest = hashlib.sha1(path.encode("utf-8")).hexdigest()[:8]
        name_path = os.path.join(os.path.expanduser("~"), ".claude", "chatter-session-names", f"{digest}.name")
        try:
            with open(name_path, "r", encoding="utf-8") as f:
                return f.read().strip()
        except OSError:
            return ""

    @classmethod
    def resolve_session_name(
        cls,
        explicit_name: Optional[str] = None,
        project_path: Optional[str] = None,
        db_path: Optional[str] = None
    ) -> str:
        if explicit_name:
            return explicit_name
        env_name = os.environ.get("CHATTER_SESSION_NAME")
        if env_name:
            return env_name

        path = project_path or os.environ.get("CHATTER_PROJECT_PATH") or os.getcwd()
        digest = hashlib.sha1(path.encode("utf-8")).hexdigest()[:8]

        sessions_dir = os.path.join(os.path.expanduser("~"), ".claude", "sessions")
        live_candidates = set()
        if os.path.exists(sessions_dir):
            for desc_file in glob.glob(os.path.join(sessions_dir, "bridge-*.json")):
                try:
                    with open(desc_file, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    raw_pid = data.get("pid")
                    if not raw_pid or not ClaudeWire.is_pid_alive(raw_pid, data.get("procStart")):
                        continue
                    if data.get("kind") == "interactive":
                        continue
                    desc_proj = data.get("projectPath") or data.get("cwd") or ""
                    desc_digest = data.get("cwdDigest") or ""
                    if desc_digest == digest or (desc_proj and os.path.realpath(desc_proj) == os.path.realpath(path)):
                        name = data.get("name")
                        if name:
                            live_candidates.add(name)
                except Exception:
                    continue

        if len(live_candidates) == 1:
            return next(iter(live_candidates))
        elif len(live_candidates) > 1:
            candidates_str = ", ".join(sorted(live_candidates))
            sys.stderr.write(
                f"Multiple live bridge sessions found for this workspace ({candidates_str}). "
                f"Please specify --name to disambiguate.\n"
            )
            return ""

        db = db_path or os.environ.get("CHATTER_DB_PATH") or os.path.join(os.path.expanduser("~"), ".claude", "chatter-messages.db")
        if os.path.exists(db):
            try:
                uri = f"file:{os.path.abspath(db)}?mode=ro"
                with sqlite3.connect(uri, uri=True, timeout=2.0) as conn:
                    cur = conn.execute(
                        "SELECT current_name FROM sessions WHERE cwd_digest = ? ORDER BY updated_at DESC, created_at DESC LIMIT 1",
                        (digest,)
                    )
                    row = cur.fetchone()
                    if row and row[0]:
                        return row[0]
            except Exception:
                pass

        persisted = cls.read_persisted_session_name(path)
        if persisted:
            return persisted

        return ""

    def _project_path(self) -> str:
        return self._find_project_path()

    def _cwd_digest(self) -> str:
        return hashlib.sha1(self._project_path().encode("utf-8")).hexdigest()[:8]

    def _persisted_name_path(self, cwd_digest: str) -> str:
        d = os.path.join(self.home_dir, ".claude", "chatter-session-names")
        try:
            os.makedirs(d, exist_ok=True)
        except OSError:
            pass
        return os.path.join(d, f"{cwd_digest}.name")

    def _persisted_id_path(self, cwd_digest: str) -> str:
        d = os.path.join(self.home_dir, ".claude", "chatter-session-names")
        try:
            os.makedirs(d, exist_ok=True)
        except OSError:
            pass
        return os.path.join(d, f"{cwd_digest}.id")

    def _derive_session_id(self) -> str:
        id_path = self._persisted_id_path(self._cwd_digest())
        try:
            with open(id_path, "r", encoding="utf-8") as f:
                sid = f.read().strip()
                if sid:
                    return sid
        except OSError:
            pass
        sid = str(uuid.uuid4())
        if self.as_bridge and not self.read_only:
            try:
                with open(id_path, "w", encoding="utf-8") as f:
                    f.write(sid)
            except OSError:
                pass
        return sid

    def _derive_default_session_name(self) -> str:
        try:
            with open(self._persisted_name_path(self._cwd_digest()), "r", encoding="utf-8") as f:
                persisted = f.read().strip()
        except OSError:
            persisted = ""
        if not persisted:
            persisted = self._random_session_name()
            if self.as_bridge and not self.read_only:
                self.persist_default_session_name(persisted)
        if self._session_name_in_use(persisted, wait_if_same_cwd=self.as_bridge):
            self._is_clashing = True
            return self._random_session_name()
        return persisted

    def _random_session_name(self) -> str:
        basename = re.sub(r"[^A-Za-z0-9._-]", "-", os.path.basename(self._project_path())) or "root"
        suffix = "".join(secrets.choice(string.ascii_lowercase + string.digits) for _ in range(2))
        return f"{basename}-{suffix}"

    @staticmethod
    def _static_is_pid_alive(pid: Optional[int], proc_start: Optional[str] = None) -> bool:
        return ClaudeWire.is_pid_alive(pid, proc_start)

    def _is_pid_alive(self, pid: Optional[int], proc_start: Optional[str] = None) -> bool:
        return ClaudeWire.is_pid_alive(pid, proc_start)

    def _session_name_in_use(
        self,
        name: str,
        wait_if_same_cwd: bool = False,
        max_wait: float = 1.5,
        poll_interval: float = 0.1
    ) -> bool:
        start_time = time.time()
        while True:
            in_use_by_other = False
            same_cwd = False
            for path in glob.glob(os.path.join(self.sessions_dir, "*.json")):
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        meta = json.load(f)
                except (OSError, ValueError):
                    continue
                pid = meta.get("pid")
                proc_start = meta.get("procStart")
                if meta.get("name") == name and pid != self.pid and self._is_pid_alive(pid, proc_start):
                    in_use_by_other = True
                    desc_proj = meta.get("projectPath") or meta.get("cwd") or ""
                    desc_digest = meta.get("cwdDigest") or ""
                    if (desc_digest and desc_digest == self._cwd_digest()) or (
                        desc_proj and os.path.realpath(desc_proj) == os.path.realpath(self._project_path())
                    ):
                        same_cwd = True
                    break

            if not in_use_by_other:
                return False

            if not wait_if_same_cwd or not same_cwd or (time.time() - start_time) >= max_wait:
                return True

            time.sleep(poll_interval)

    def persist_default_session_name(self, name: str):
        try:
            with open(self._persisted_name_path(self._cwd_digest()), "w", encoding="utf-8") as f:
                f.write(name)
            logger.info(f"Persisted '{name}' as this workspace's default session name.")
        except OSError as e:
            logger.error(f"Failed to persist session name '{name}': {e}")

    def persist_session_id(self, session_id: str):
        try:
            with open(self._persisted_id_path(self._cwd_digest()), "w", encoding="utf-8") as f:
                f.write(session_id)
            logger.info(f"Persisted '{session_id}' as this workspace's session id.")
        except OSError as e:
            logger.error(f"Failed to persist session id '{session_id}': {e}")

    def _get_default_socket_path(self, pid: int, session_name: Optional[str] = None) -> str:
        sname = (session_name or getattr(self, "session_name", "bridge")).replace("/", "-")
        try:
            uid = os.getuid()
            cc_socks_dir = f"/run/user/{uid}/cc-socks"
            if os.path.exists(cc_socks_dir) and os.access(cc_socks_dir, os.W_OK):
                return os.path.join(cc_socks_dir, f"bridge-{sname}.sock")

            fallback_dir = "/tmp/cc-socks"
            os.makedirs(fallback_dir, exist_ok=True)
            return os.path.join(fallback_dir, f"bridge-{sname}.sock")
        except Exception:
            pass
        return f"/tmp/bridge_{sname}.sock"

    async def verify_and_purge_sessions(self) -> Dict[str, Dict[str, Any]]:
        """
        Scans workspace configurations, checks if tracking PIDs are alive,
        and systematically cleans up orphaned or dead sockets and key files.
        """
        logger.info("Running proactive session structural analysis...")
        if not os.path.exists(self.sessions_dir):
            logger.warning(f"Sessions directory does not exist: {self.sessions_dir}")
            self.active_peers = {}
            return {}

        live_by_pid: Dict[int, Dict[str, Any]] = {}
        json_files = sorted(glob.glob(os.path.join(self.sessions_dir, "*.json")))
        purged_count = 0

        for file_path in json_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)

                pid = meta.get("pid")
                proc_start = meta.get("procStart")
                sock_path = meta.get("messagingSocketPath")
                name = meta.get("name", f"session-{pid}")
                session_id = meta.get("sessionId", "")
                cwd = meta.get("cwd", "")

                if pid and self._is_pid_alive(pid, proc_start):
                    key_pattern = os.path.join(self.sessions_dir, f"{pid}.*.key")
                    discovered_keys = glob.glob(key_pattern)

                    token = ""
                    if discovered_keys:
                        try:
                            with open(discovered_keys[0], "r", encoding="utf-8") as kf:
                                key_raw = kf.read().strip()
                                try:
                                    key_data = json.loads(key_raw)
                                    if isinstance(key_data, dict) and "peerToken" in key_data:
                                        token = key_data["peerToken"]
                                    else:
                                        token = key_raw
                                except json.JSONDecodeError:
                                    token = key_raw
                        except Exception as kerr:
                            logger.error(f"Failed to read key file {discovered_keys[0]}: {kerr}")

                    live_by_pid[pid] = {
                        "name": name,
                        "pid": pid,
                        "sessionId": session_id,
                        "socket": sock_path,
                        "token": token,
                        "cwd": cwd,
                        "kind": meta.get("kind", "interactive"),
                        "updatedAt": meta.get("updatedAt"),
                        "status": meta.get("status", "active"),
                        "jobId": meta.get("jobId"),
                        "parkedJobId": meta.get("parkedJobId"),
                    }
                else:
                    logger.warning(f"Purging dead session files for PID {pid}")
                    try:
                        os.remove(file_path)
                        purged_count += 1
                    except OSError as e:
                        logger.error(f"Failed to remove stale session file {file_path}: {e}")

                    if pid:
                        key_pattern = os.path.join(self.sessions_dir, f"{pid}.*.key")
                        for key_file in glob.glob(key_pattern):
                            try:
                                os.remove(key_file)
                                logger.info(f"Purged stale key file: {key_file}")
                            except OSError as e:
                                logger.error(f"Failed to remove stale key file {key_file}: {e}")
            except Exception as e:
                logger.error(f"Error checking session file {file_path}: {e}")

        current_sessions = ClaudeWire.index_peers(live_by_pid.values())
        self.active_peers = current_sessions
        if self.store:
            self.store.prune()
        logger.info(f"Session analysis complete. Active peers: {len(current_sessions)}, Purged: {purged_count}")
        return self.active_peers

    def list_sessions(self, redact_tokens: bool = True) -> Dict[str, Dict[str, Any]]:
        peers = ClaudeWire.scan_sessions(self.sessions_dir)
        if redact_tokens:
            return ClaudeWire.redact_peers(peers)
        return peers

    @staticmethod
    def redact_peers(peers: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        return ClaudeWire.redact_peers(peers)

    def _resolve_session(self, session_identifier: str) -> Optional[Dict[str, Any]]:
        if not self.active_peers:
            return None

        if session_identifier in self.active_peers:
            return self.active_peers[session_identifier]

        for peer in self.active_peers.values():
            if str(peer.get("pid")) == str(session_identifier):
                return peer

        for peer in self.active_peers.values():
            if peer.get("sessionId") == session_identifier:
                return peer

        same_name = [p for p in self.active_peers.values() if p.get("name") == session_identifier]
        if len(same_name) > 1:
            raise AmbiguousSessionError(session_identifier, same_name)

        session_lower = session_identifier.lower()
        partial = [p for p in self.active_peers.values() if session_lower in p["name"].lower()]
        if len(partial) > 1:
            raise AmbiguousSessionError(session_identifier, partial)
        return partial[0] if partial else None

    async def send_message(
        self,
        target_session: Optional[str] = None,
        message_content: Optional[str] = None,
        *,
        session_identifier: Optional[str] = None,
        priority: str = "next"
    ) -> Dict[str, Any]:
        """
        Dispatches a message frame to a verified target session via UNIX socket.
        Accepts target_session or legacy session_identifier keyword argument.
        """
        target = target_session or session_identifier
        if not target:
            return {"success": False, "error": "Target session identifier required."}
        if message_content is None:
            return {"success": False, "error": "Message content required."}

        await self.verify_and_purge_sessions()

        try:
            peer = self._resolve_session(target)
        except AmbiguousSessionError as e:
            return {
                "success": False,
                "error": f"Target '{target}' matches {len(e.candidates)} live sessions; "
                         f"address one by pid or sessionId.",
                "candidates": [
                    {"name": c["name"], "pid": c["pid"], "sessionId": c.get("sessionId"), "cwd": c.get("cwd")}
                    for c in e.candidates
                ],
            }
        if not peer:
            logger.error(f"Target session '{target}' is not reachable.")
            return {
                "success": False,
                "error": f"Target session '{target}' not found among active sessions: {list(self.active_peers.keys())}"
            }

        socket_path = peer.get("socket")
        if not socket_path or not os.path.exists(socket_path):
            logger.error(f"Socket path for '{peer['name']}' does not exist: {socket_path}")
            return {
                "success": False,
                "error": f"Socket path '{socket_path}' for session '{peer['name']}' does not exist on disk."
            }

        msg_id = str(uuid.uuid4())
        from_uri = f"uds:{self.bridge_socket_path}"
        wrapped_content = (
            f'<cross-session-message from="{from_uri}" from-name="{self.session_name}" '
            f'from-mode="prompting">\n{message_content}\n</cross-session-message>'
        )
        message_frame = {
            "msgV": 1,
            "msg_id": msg_id,
            "type": "user",
            "message": {"role": "user", "content": wrapped_content},
            "priority": priority,
            "from": from_uri
        }

        try:
            reader, writer = await asyncio.open_unix_connection(socket_path)
            writer.write((json.dumps(message_frame) + "\n").encode("utf-8"))
            await writer.drain()

            try:
                ack_line = await asyncio.wait_for(reader.readline(), timeout=2.0)
                if ack_line:
                    logger.info(f"Received instant ACK from {peer['name']}: {ack_line.decode('utf-8').strip()}")
            except (asyncio.TimeoutError, Exception):
                pass

            writer.close()
            await writer.wait_closed()

            entry = {
                "direction": "outbound",
                "status": "dispatched",
                "msg_id": msg_id,
                "target_session": peer["name"],
                "target_pid": peer["pid"],
                "content": message_content,
                "response": "",
                "timestamp": time.time()
            }
            self.response_store[msg_id] = entry
            logger.info(f"Successfully dispatched message {msg_id} to {peer['name']}")

            return {
                "success": True,
                "msg_id": msg_id,
                "target": peer["name"],
                "status": "dispatched",
                "detail": entry
            }

        except Exception as e:
            logger.error(f"IPC injection crash sending to {socket_path}: {e}")
            return {
                "success": False,
                "error": f"Failed to send message over Unix socket to {socket_path}: {e}"
            }

    send_to_claude = send_message

    def register_session_descriptor(self, session_name: str = "bridge", kind: str = "bg"):
        sanitized = session_name or getattr(self, "session_name", "bridge")
        json_p, key_p, named_p, tok = ClaudeWire.register_descriptor(
            sessions_dir=self.sessions_dir,
            session_name=sanitized,
            pid=self.pid,
            socket_path=self.bridge_socket_path,
            kind=kind,
            project_path=self._project_path(),
            cwd_digest=self._cwd_digest(),
            session_id=getattr(self, "session_id", None)
        )
        self.session_json_path = json_p
        self.session_key_path = key_p
        self.named_descriptor_path = named_p
        self.registered_token = tok
        self.registered_pid = self.pid
        logger.info(f"Registered session descriptor '{sanitized}' (PID {self.pid}) in {self.sessions_dir}")

    def cleanup_session_descriptor(self):
        ClaudeWire.cleanup_descriptor(
            getattr(self, "session_json_path", None),
            getattr(self, "session_key_path", None),
            getattr(self, "named_descriptor_path", None)
        )

    async def handle_inbound_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            while not reader.at_eof():
                line = await reader.readline()
                if not line:
                    break
                line_str = line.decode("utf-8").strip()
                if not line_str:
                    continue

                try:
                    payload = json.loads(line_str)
                    frame_type = payload.get("type")

                    if frame_type == "auth":
                        logger.info("Inbound peer authenticated successfully.")
                        continue

                    msg_id = payload.get("msg_id", f"inbound_{uuid.uuid4()}")

                    msg_obj = payload.get("message", {})
                    if isinstance(msg_obj, dict):
                        content_val = msg_obj.get("content", payload.get("content", ""))
                    else:
                        content_val = payload.get("content", str(msg_obj))

                    sender = payload.get("sender")
                    wrapper_match = re.match(
                        r'^<cross-session-message\s+([^>]*)>\n(.*)\n</cross-session-message>$',
                        content_val, re.DOTALL
                    )
                    if wrapper_match:
                        attrs, inner_text = wrapper_match.groups()
                        if sender is None:
                            name_match = re.search(r'from-name="([^"]*)"', attrs)
                            sender = name_match.group(1) if name_match else payload.get("from", "unknown")
                        content_val = inner_text
                    if sender is None:
                        sender = payload.get("from", "unknown")

                    now = time.time()
                    event = {
                        "msg_id": msg_id,
                        "sender": sender,
                        "content": content_val,
                        "timestamp": now,
                        "payload": payload
                    }

                    self.response_store[msg_id] = {
                        "direction": "inbound",
                        "read": False,
                        "status": payload.get("status", "received"),
                        "sender": sender,
                        "content": content_val,
                        "payload": payload,
                        "timestamp": now
                    }
                    logger.info(f"Buffered inbound response frame: {msg_id}")

                    if self.on_inbound_message:
                        try:
                            res = self.on_inbound_message(event)
                            if inspect.isawaitable(res):
                                task = asyncio.ensure_future(res)
                                self._background_tasks.add(task)

                                def _done(t):
                                    self._background_tasks.discard(t)
                                    if not t.cancelled() and t.exception():
                                        logger.error(f"Inbound callback task failed: {t.exception()}")
                                task.add_done_callback(_done)
                        except Exception as cb_err:
                            logger.error(f"Inbound callback exception: {cb_err}")

                except json.JSONDecodeError:
                    logger.error("Received invalid JSON payload over inbound bridge socket")
                except Exception as ex:
                    logger.error(f"Error processing inbound frame payload: {ex}")
        except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
            logger.info("Inbound peer connection closed.")
        except Exception as e:
            logger.error(f"Error handling inbound traffic: {e}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    def get_unread_messages(self, mark_read: bool = True) -> Dict[str, Dict[str, Any]]:
        unread = {
            msg_id: entry
            for msg_id, entry in self.response_store.items()
            if entry.get("direction") == "inbound" and not entry.get("read", False)
        }
        unread = dict(sorted(unread.items(), key=lambda kv: kv[1].get("timestamp", 0)))
        if mark_read:
            to_mark = []
            for msg_id, entry in unread.items():
                entry["read"] = True
                to_mark.append(msg_id)
            if to_mark and isinstance(self.response_store, ResponseStore):
                self.response_store.mark_read(to_mark)
        return unread

    def get_history(self, msg_id: Optional[str] = None, limit: Optional[int] = None, offset: int = 0) -> Dict[str, Any]:
        if msg_id:
            entry = self.response_store.get(msg_id)
            data = {msg_id: entry} if entry else {}
            res = {"history": data}
            if self.persistence_error:
                res["persistence_error"] = self.persistence_error
            return res

        items = list(self.response_store.items())
        items.sort(key=lambda kv: kv[1].get("timestamp", 0), reverse=True)
        if offset > 0:
            items = items[offset:]
        if limit is not None:
            items = items[:limit]
        history_dict = dict(items)
        res = {"history": history_dict}
        if self.persistence_error:
            res["persistence_error"] = self.persistence_error
        return res

    def rename_session_in_store(self, old_name: str, new_name: str) -> Optional[str]:
        if self.store:
            self.session_id = self.store.rename_session(
                old_name,
                new_name,
                session_id=getattr(self, "session_id", None)
            )
        self.persist_default_session_name(new_name)
        if getattr(self, "session_id", None):
            self.persist_session_id(self.session_id)
        if isinstance(self.response_store, ResponseStore):
            self.response_store.set_session_name(new_name, getattr(self, "session_id", None))
        return self.session_id

    def rename_session(self, new_name: str) -> Dict[str, Any]:
        if self.session_name == new_name:
            return {
                "success": True,
                "message": f"Session is already named '{new_name}'",
                "previous_name": self.session_name,
                "new_name": new_name,
                "pid": self.pid,
                "descriptor_file": getattr(self, "session_json_path", "")
            }
        if self._session_name_in_use(new_name):
            raise SessionNameInUseError(f"Cannot rename: session name '{new_name}' is currently in use by an active session.")

        old_name = self.session_name
        try:
            self.rename_session_in_store(old_name, new_name)
        except Exception as e:
            raise StorageError(f"Failed to rename session in storage: {e}") from e

        self.cleanup_session_descriptor()
        self.session_name = new_name
        self.register_session_descriptor(session_name=new_name)
        return {
            "success": True,
            "previous_name": old_name,
            "new_name": new_name,
            "pid": self.pid,
            "descriptor_file": getattr(self, "session_json_path", "")
        }

    async def start_bridge_listener(self):
        if os.path.exists(self.bridge_socket_path):
            try:
                os.remove(self.bridge_socket_path)
            except OSError as e:
                logger.error(f"Failed to remove stale bridge socket {self.bridge_socket_path}: {e}")

        try:
            server = await asyncio.start_unix_server(
                self.handle_inbound_client, self.bridge_socket_path
            )
            os.chmod(self.bridge_socket_path, stat.S_IRUSR | stat.S_IWUSR)
            logger.info(f"Bridge server operational on: {self.bridge_socket_path}")

            async with server:
                await server.serve_forever()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Bridge listener server error: {e}")

    start_listener = start_bridge_listener

    def cleanup_socket(self):
        for task in list(self._background_tasks):
            if not task.done():
                task.cancel()
        self._background_tasks.clear()

        if self.store:
            self.store.close()
        if os.path.exists(self.bridge_socket_path):
            try:
                os.remove(self.bridge_socket_path)
                logger.info(f"Cleaned up socket file {self.bridge_socket_path}")
            except OSError as e:
                logger.error(f"Error removing socket file {self.bridge_socket_path}: {e}")
