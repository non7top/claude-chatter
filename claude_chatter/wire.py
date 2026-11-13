"""
Stateless wire protocol and read-only session discovery for Claude Code.

Handles session descriptor parsing from ~/.claude/sessions, process liveness
verification via /proc/<pid>/stat and psutil, and NDJSON frame serialization
over UNIX domain sockets.
"""
import os
import sys
import glob
import json
import time
import uuid
import asyncio
import logging
import socket
import struct
from collections import Counter
from typing import Any, Dict, Iterable, Optional, Tuple

import psutil

logger = logging.getLogger("claude_chatter")


class ClaudeWire:
    """Stateless wire protocol operations and read-only session discovery."""

    @staticmethod
    def is_pid_alive(pid: Optional[int], proc_start: Optional[str] = None) -> bool:
        """
        Verifies if a process PID is currently alive.

        On Linux, when proc_start is provided and /proc/<pid>/stat exists,
        verifies that field 22 (starttime) matches proc_start to guard against
        PID reuse.

        Fallback checks psutil.pid_exists(pid) and status != zombie.
        """
        if not pid or not isinstance(pid, int):
            return False

        if sys.platform.startswith("linux") and proc_start:
            stat_path = f"/proc/{pid}/stat"
            if os.path.exists(stat_path):
                try:
                    with open(stat_path, "r", encoding="utf-8") as f:
                        after_comm = f.read().rsplit(")", 1)[1].split()
                        actual_start = after_comm[19]
                        if actual_start != str(proc_start):
                            return False
                except (OSError, IndexError):
                    return False

        try:
            proc = psutil.Process(pid)
            return proc.status() != psutil.STATUS_ZOMBIE
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return False
        except Exception:
            return False

    @classmethod
    def local_pid_domain(cls) -> Optional[str]:
        """This process's pidDomain, or None where descriptors' pidDomain can't be compared (non-Linux)."""
        return cls.compute_pid_domain(os.getpid()) if sys.platform.startswith("linux") else None

    @staticmethod
    def in_foreign_pid_domain(meta: Dict[str, Any], local_domain: Optional[str]) -> bool:
        """True for a descriptor whose PID belongs to another PID namespace or machine."""
        domain = meta.get("pidDomain")
        return bool(local_domain and domain and domain != local_domain)

    @staticmethod
    def peer_pid(sock: Any) -> Optional[int]:
        """PID of the process on the other end of a connected unix socket (Linux SO_PEERCRED), else None."""
        option = getattr(socket, "SO_PEERCRED", None)
        if sock is None or option is None:
            return None
        try:
            pid, _uid, _gid = struct.unpack("3i", sock.getsockopt(socket.SOL_SOCKET, option, struct.calcsize("3i")))
        except (OSError, struct.error):
            return None
        return pid or None

    @staticmethod
    def match_session_by_pid(pid: int, sessions: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """The live session that is, or is an ancestor of, the given process."""
        by_pid = {s["pid"]: s for s in sessions.values()}
        if pid in by_pid:
            return by_pid[pid]
        try:
            for parent in psutil.Process(pid).parents():
                if parent.pid in by_pid:
                    return by_pid[parent.pid]
        except psutil.Error:
            pass
        return None

    @staticmethod
    def index_peers(peers: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Keys peers by name; a name shared by several sessions is keyed name#pid for each."""
        peers = list(peers)
        counts = Counter(p["name"] for p in peers)
        return {
            (p["name"] if counts[p["name"]] == 1 else f"{p['name']}#{p['pid']}"): p
            for p in peers
        }

    @classmethod
    def scan_sessions(cls, sessions_dir: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
        """
        Read-only scan of session descriptors in ~/.claude/sessions.
        Validates process liveness without unlinking or modifying any files.
        """
        if not sessions_dir:
            sessions_dir = os.path.join(os.path.expanduser("~"), ".claude", "sessions")

        if not os.path.exists(sessions_dir):
            return {}

        live_by_pid: Dict[int, Dict[str, Any]] = {}
        local_domain = cls.local_pid_domain()
        for file_path in sorted(glob.glob(os.path.join(sessions_dir, "*.json"))):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)

                if cls.in_foreign_pid_domain(meta, local_domain):
                    continue

                pid = meta.get("pid")
                proc_start = meta.get("procStart")

                if pid and pid not in live_by_pid and cls.is_pid_alive(pid, proc_start):
                    name = meta.get("name", f"session-{pid}")
                    key_pattern = os.path.join(sessions_dir, f"{pid}.*.key")
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
                        "sessionId": meta.get("sessionId", ""),
                        "socket": meta.get("messagingSocketPath"),
                        "token": token,
                        "cwd": meta.get("cwd", ""),
                        "kind": meta.get("kind", "interactive"),
                        "updatedAt": meta.get("updatedAt"),
                        "status": meta.get("status", "active"),
                        "jobId": meta.get("jobId"),
                        "parkedJobId": meta.get("parkedJobId"),
                        "descriptorFile": file_path,
                    }
            except Exception as e:
                logger.error(f"Error reading session file {file_path}: {e}")

        return cls.index_peers(live_by_pid.values())

    @staticmethod
    def redact_peers(peers: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Returns a copy of peers without authentication tokens."""
        return {n: {k: v for k, v in p.items() if k != "token"} for n, p in peers.items()}

    @staticmethod
    def compute_pid_domain(pid: int) -> str:
        """
        Builds the pidDomain field matching real Claude Code descriptors:
        linux:<machine-id>:pid:[<namespace-inode>]
        """
        try:
            with open("/etc/machine-id", "r", encoding="utf-8") as f:
                machine_id = f.read().strip()
        except OSError:
            machine_id = "local"
        try:
            ns_link = os.readlink(f"/proc/{pid}/ns/pid")
            ns_part = ns_link.split(":", 1)[1] if ":" in ns_link else f"[{pid}]"
        except OSError:
            ns_part = f"[{pid}]"
        return f"linux:{machine_id}:pid:{ns_part}"

    @staticmethod
    def compute_proc_start(pid: int) -> str:
        """Extracts field 22 (starttime) from /proc/<pid>/stat."""
        try:
            with open(f"/proc/{pid}/stat", "r", encoding="utf-8") as f:
                after_comm = f.read().rsplit(")", 1)[1].split()
                return after_comm[19]
        except (OSError, IndexError):
            return str(pid)

    @classmethod
    def register_descriptor(
        cls,
        sessions_dir: str,
        session_name: str,
        pid: int,
        socket_path: str,
        kind: str = "bg",
        project_path: Optional[str] = None,
        cwd_digest: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> Tuple[str, str, str, str]:
        """
        Registers session descriptor and key files in sessions_dir.
        Returns (json_path, key_path, named_descriptor_path, peer_token).
        """
        os.makedirs(sessions_dir, exist_ok=True)
        now = int(time.time() * 1000)
        token = uuid.uuid4().hex
        sid = session_id or str(uuid.uuid4())
        sanitized_name = session_name.replace("/", "-")

        json_path = os.path.join(sessions_dir, f"{pid}.json")
        key_path = os.path.join(sessions_dir, f"{pid}.{token[:16]}.key")
        named_path = os.path.join(sessions_dir, f"bridge-{sanitized_name}.json")

        proc_start = cls.compute_proc_start(pid)
        pid_domain = cls.compute_pid_domain(pid)

        session_data = {
            "pid": pid,
            "sessionId": sid,
            "cwd": os.getcwd(),
            "projectPath": project_path or os.getcwd(),
            "cwdDigest": cwd_digest or "",
            "startedAt": now,
            "procStart": proc_start,
            "version": "2.1.282",
            "peerProtocol": 1,
            "peerFeatures": ["notify_idle", "reply_across_default_dirs", "artifact_yield"],
            "kind": kind,
            "entrypoint": "cli",
            "pidDomain": pid_domain,
            "messagingSocketPath": socket_path,
            "name": session_name,
            "nameSource": "user",
            "nameSince": now,
            "status": "idle",
            "updatedAt": now,
            "statusUpdatedAt": now,
        }

        key_data = {
            "peerToken": token,
            "procStart": proc_start,
            "pidDomain": pid_domain,
        }

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(session_data, f, indent=2)
        with open(named_path, "w", encoding="utf-8") as f:
            json.dump(session_data, f, indent=2)
        with open(os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w", encoding="utf-8") as f:
            json.dump(key_data, f, indent=2)

        return json_path, key_path, named_path, token

    @staticmethod
    def cleanup_descriptor(*paths: Optional[str]) -> None:
        """Removes descriptor and key files if they exist."""
        for p in paths:
            if p and os.path.exists(p):
                try:
                    os.remove(p)
                except OSError as e:
                    logger.error(f"Failed to remove descriptor file {p}: {e}")

    @staticmethod
    async def send_frame(
        socket_path: str,
        frame: Dict[str, Any],
        timeout: float = 10.0
    ) -> None:
        """
        Dispatches an NDJSON frame to target UNIX domain socket.
        Does not expect or wait for in-band reply bytes.
        """
        reader, writer = await asyncio.wait_for(
            asyncio.open_unix_connection(socket_path),
            timeout=timeout
        )
        try:
            writer.write((json.dumps(frame) + "\n").encode("utf-8"))
            await writer.drain()
            try:
                await asyncio.wait_for(reader.readline(), timeout=1.0)
            except (asyncio.TimeoutError, Exception):
                pass
        finally:
            writer.close()
            await writer.wait_closed()
