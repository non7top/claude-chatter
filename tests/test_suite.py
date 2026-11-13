#!/usr/bin/env python3
"""
Comprehensive Test Suite for Claude Code Peer Messaging Protocol and Client Library (claude-chatter)
"""
import os
import re
import sys
import json
import time
import uuid
import hashlib
import asyncio
import tempfile
import atexit
import unittest
from unittest import mock
import subprocess
import io

# Isolate host-native test runs from real ~/.claude/chatter-messages.db
_suite_tmp_db_dir = tempfile.TemporaryDirectory()
os.environ.setdefault("CHATTER_DB_PATH", os.path.join(_suite_tmp_db_dir.name, "test_suite_default.db"))
atexit.register(_suite_tmp_db_dir.cleanup)

# Ensure repo directory is in path and subprocess environment
_repo_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _repo_dir)
_curr_pythonpath = os.environ.get("PYTHONPATH", "")
os.environ["PYTHONPATH"] = f"{_repo_dir}:{_curr_pythonpath}" if _curr_pythonpath else _repo_dir
from claude_chatter import (
    AmbiguousSessionError,
    ClaudeClient,
    ClaudeMessagingProtocol,
    ClaudeWire,
    SessionNameInUseError,
    StorageError,
)


class TestBridgeSessionManagement(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp_dir.name, "test_session_mgmt.db")
        self.prev_db = os.environ.get("CHATTER_DB_PATH")
        os.environ["CHATTER_DB_PATH"] = self.db_path
        self.socket_path = os.path.join(self.tmp_dir.name, "test_session_mgmt.sock")
        self.bridge = ClaudeMessagingProtocol(bridge_socket_path=self.socket_path, db_path=self.db_path)

    async def asyncTearDown(self):
        self.bridge.cleanup_session_descriptor()
        self.bridge.cleanup_socket()
        if self.prev_db is not None:
            os.environ["CHATTER_DB_PATH"] = self.prev_db
        else:
            os.environ.pop("CHATTER_DB_PATH", None)
        self.tmp_dir.cleanup()

    async def test_session_descriptor_registration(self):
        """Verify session descriptor and key file creation & cleanup."""
        self.bridge.register_session_descriptor(session_name="test-bridge-suite")

        # Check files exist
        self.assertTrue(os.path.exists(self.bridge.session_json_path))
        self.assertTrue(os.path.exists(self.bridge.session_key_path))

        # Check json content
        with open(self.bridge.session_json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            self.assertEqual(data["name"], "test-bridge-suite")
            self.assertEqual(data["messagingSocketPath"], self.socket_path)

        # Check key content
        with open(self.bridge.session_key_path, "r", encoding="utf-8") as f:
            key_data = json.load(f)
            self.assertEqual(key_data["peerToken"], self.bridge.registered_token)

        # Cleanup
        self.bridge.cleanup_session_descriptor()
        self.assertFalse(os.path.exists(self.bridge.session_json_path))
        self.assertFalse(os.path.exists(self.bridge.session_key_path))

    async def test_session_descriptor_pid_fields_match_real_kernel_values(self):
        """Verify pidDomain/procStart are the real kernel-reported values a
        native Claude Code session writes (machine-id + PID-namespace inode;
        /proc/<pid>/stat's starttime field), not placeholders derived from
        the literal pid - a real session's descriptor was observed to reject
        cross-session delivery to a bridge using the old placeholder shape."""
        self.bridge.register_session_descriptor(session_name="test-pid-fields")
        try:
            with open(self.bridge.session_json_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            try:
                with open("/etc/machine-id", "r", encoding="utf-8") as f:
                    machine_id = f.read().strip()
            except OSError:
                machine_id = "local"  # e.g. inside this project's own minimal test container
            ns_part = os.readlink(f"/proc/{os.getpid()}/ns/pid").split(":", 1)[1]
            self.assertEqual(data["pidDomain"], f"linux:{machine_id}:pid:{ns_part}")

            with open(f"/proc/{os.getpid()}/stat", "r", encoding="utf-8") as f:
                expected_proc_start = f.read().rsplit(")", 1)[1].split()[19]
            self.assertEqual(data["procStart"], expected_proc_start)
            self.assertNotEqual(data["procStart"], str(os.getpid()))

            with open(self.bridge.session_key_path, "r", encoding="utf-8") as f:
                key_data = json.load(f)
            self.assertEqual(key_data["procStart"], data["procStart"])
        finally:
            self.bridge.cleanup_session_descriptor()

    def _name_file(self, project_path):
        digest = hashlib.sha1(project_path.encode("utf-8")).hexdigest()[:8]
        return os.path.expanduser(f"~/.claude/chatter-session-names/{digest}.name")

    async def test_default_name_is_basename_plus_two_random_chars_and_persisted(self):
        workspace = tempfile.mkdtemp(prefix="persist_name_test_")
        old_cwd = os.getcwd()
        os.chdir(workspace)
        try:
            first = ClaudeMessagingProtocol(bridge_socket_path=os.path.join(self.tmp_dir.name, "p1.sock"))
            self.assertRegex(first.session_name, rf"^{re.escape(os.path.basename(workspace))}-[a-z0-9]{{2}}$")
            with open(self._name_file(workspace), "r", encoding="utf-8") as f:
                self.assertEqual(f.read(), first.session_name)

            second = ClaudeMessagingProtocol(bridge_socket_path=os.path.join(self.tmp_dir.name, "p2.sock"))
            self.assertEqual(second.session_name, first.session_name)
        finally:
            os.chdir(old_cwd)
            if os.path.exists(self._name_file(workspace)):
                os.remove(self._name_file(workspace))

    async def test_rename_overwrites_the_persisted_name(self):
        workspace = tempfile.mkdtemp(prefix="persist_name_test_")
        old_cwd = os.getcwd()
        os.chdir(workspace)
        try:
            first = ClaudeMessagingProtocol(bridge_socket_path=os.path.join(self.tmp_dir.name, "p1.sock"))
            first.persist_default_session_name("gwen")
            second = ClaudeMessagingProtocol(bridge_socket_path=os.path.join(self.tmp_dir.name, "p2.sock"))
            self.assertEqual(second.session_name, "gwen")
        finally:
            os.chdir(old_cwd)
            if os.path.exists(self._name_file(workspace)):
                os.remove(self._name_file(workspace))

    async def test_project_path_comes_from_env_or_explicit(self):
        project = tempfile.mkdtemp(prefix="project_")
        try:
            with mock.patch.dict(os.environ, {"CHATTER_PROJECT_PATH": project}):
                protocol = ClaudeMessagingProtocol(bridge_socket_path=os.path.join(self.tmp_dir.name, "a.sock"))
            self.assertRegex(protocol.session_name, rf"^{re.escape(os.path.basename(project))}-[a-z0-9]{{2}}$")
            self.assertTrue(os.path.exists(self._name_file(project)))
        finally:
            if os.path.exists(self._name_file(project)):
                os.remove(self._name_file(project))

    async def test_stored_name_in_use_by_a_live_session_gets_a_fresh_unpersisted_suffix(self):
        workspace = tempfile.mkdtemp(prefix="name_clash_test_")
        old_cwd = os.getcwd()
        os.chdir(workspace)
        sessions_dir = self.bridge.sessions_dir
        os.makedirs(sessions_dir, exist_ok=True)
        other = subprocess.Popen(["sleep", "30"])
        other_json = os.path.join(sessions_dir, f"{other.pid}.clash-test.json")
        try:
            first = ClaudeMessagingProtocol(bridge_socket_path=os.path.join(self.tmp_dir.name, "c1.sock"))
            stored = first.session_name
            with open(other_json, "w", encoding="utf-8") as f:
                json.dump({"pid": other.pid, "name": stored}, f)
            clashing = ClaudeMessagingProtocol(bridge_socket_path=os.path.join(self.tmp_dir.name, "c2.sock"))
            self.assertNotEqual(clashing.session_name, stored)
            with open(self._name_file(workspace), "r", encoding="utf-8") as f:
                self.assertEqual(f.read(), stored)
        finally:
            other.kill()
            other.wait()
            os.chdir(old_cwd)
            for path in (other_json, self._name_file(workspace)):
                if os.path.exists(path):
                    os.remove(path)

    async def test_dead_session_purging(self):
        """Verify purging of dead session descriptors and key files."""
        sessions_dir = self.bridge.sessions_dir
        os.makedirs(sessions_dir, exist_ok=True)

        fake_pid = 9999999
        fake_json = os.path.join(sessions_dir, f"{fake_pid}.json")
        fake_key = os.path.join(sessions_dir, f"{fake_pid}.abcd1234key.key")

        with open(fake_json, "w", encoding="utf-8") as f:
            json.dump({"pid": fake_pid, "messagingSocketPath": "/tmp/fake.sock", "name": "dead-session"}, f)

        with open(fake_key, "w", encoding="utf-8") as f:
            json.dump({"peerToken": "fake_token", "procStart": str(fake_pid)}, f)

        self.assertTrue(os.path.exists(fake_json))
        self.assertTrue(os.path.exists(fake_key))

        # Run verification and purging
        await self.bridge.verify_and_purge_sessions()

        self.assertFalse(os.path.exists(fake_json))
        self.assertFalse(os.path.exists(fake_key))


class TestUnreadMessages(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp_dir.name, "unread_test.db")
        self.bridge = ClaudeMessagingProtocol(
            bridge_socket_path=os.path.join(self.tmp_dir.name, "test.sock"),
            db_path=self.db_path
        )

    async def asyncTearDown(self):
        self.bridge.cleanup_socket()
        self.tmp_dir.cleanup()

    async def test_get_unread_messages_marks_read_and_excludes_outbound(self):
        """Verify get_unread_messages returns only unread inbound entries, marks
        them read by default, and never surfaces our own outbound dispatch
        confirmations (which share the same response_store but aren't inbox
        items)."""
        self.bridge.response_store["out_1"] = {
            "direction": "outbound", "status": "dispatched", "content": "hi"
        }
        self.bridge.response_store["in_1"] = {
            "direction": "inbound", "read": False, "sender": "peer-a",
            "content": "first", "timestamp": 1.0
        }
        self.bridge.response_store["in_2"] = {
            "direction": "inbound", "read": False, "sender": "peer-b",
            "content": "second", "timestamp": 2.0
        }

        unread = self.bridge.get_unread_messages()
        self.assertEqual(list(unread.keys()), ["in_1", "in_2"])
        self.assertTrue(self.bridge.response_store["in_1"]["read"])
        self.assertTrue(self.bridge.response_store["in_2"]["read"])

        # A second call should now return nothing - already marked read.
        self.assertEqual(self.bridge.get_unread_messages(), {})

    async def test_get_unread_messages_peek_without_consuming(self):
        """Verify mark_read=False lets a caller peek without consuming."""
        self.bridge.response_store["in_1"] = {
            "direction": "inbound", "read": False, "sender": "peer-a",
            "content": "first", "timestamp": 1.0
        }
        unread = self.bridge.get_unread_messages(mark_read=False)
        self.assertEqual(list(unread.keys()), ["in_1"])
        self.assertFalse(self.bridge.response_store["in_1"]["read"])
        # still there on the next real read
        self.assertEqual(list(self.bridge.get_unread_messages().keys()), ["in_1"])


class TestMessagePersistenceAndSQLite(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp_dir.name, "test_messages.db")
        self.sock_path = os.path.join(self.tmp_dir.name, "test_bridge.sock")
        self.session_name = "test-session-persist"

    async def asyncTearDown(self):
        self.tmp_dir.cleanup()

    async def test_inbound_message_persists_across_bridge_restart(self):
        bridge1 = ClaudeMessagingProtocol(
            bridge_socket_path=self.sock_path,
            session_name=self.session_name,
            db_path=self.db_path
        )
        server_task = asyncio.create_task(bridge1.start_bridge_listener())
        await asyncio.sleep(0.1)

        reader, writer = await asyncio.open_unix_connection(self.sock_path)
        msg_id = "persist_in_1"
        frame = {
            "msgV": 1,
            "msg_id": msg_id,
            "type": "user",
            "message": {
                "role": "user",
                "content": "Hello from peer"
            },
            "sender": "sender_session"
        }
        writer.write((json.dumps(frame) + "\n").encode("utf-8"))
        await writer.drain()
        await asyncio.sleep(0.1)
        writer.close()
        await writer.wait_closed()

        self.assertIn(msg_id, bridge1.response_store)
        self.assertFalse(bridge1.response_store[msg_id]["read"])

        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass
        bridge1.cleanup_socket()

        bridge2 = ClaudeMessagingProtocol(
            bridge_socket_path=self.sock_path,
            session_name=self.session_name,
            db_path=self.db_path
        )
        self.assertIn(msg_id, bridge2.response_store)
        unread = bridge2.get_unread_messages(mark_read=False)
        self.assertIn(msg_id, unread)
        self.assertEqual(unread[msg_id]["content"], "Hello from peer")
        self.assertEqual(unread[msg_id]["sender"], "sender_session")
        self.assertFalse(unread[msg_id]["read"])

        consumed = bridge2.get_unread_messages(mark_read=True)
        self.assertIn(msg_id, consumed)
        self.assertTrue(bridge2.response_store[msg_id]["read"])
        bridge2.cleanup_socket()

        bridge3 = ClaudeMessagingProtocol(
            bridge_socket_path=self.sock_path,
            session_name=self.session_name,
            db_path=self.db_path
        )
        self.assertEqual(bridge3.get_unread_messages(), {})
        self.assertIn(msg_id, bridge3.response_store)
        self.assertTrue(bridge3.response_store[msg_id]["read"])
        bridge3.cleanup_socket()

    async def test_outbound_message_persists_across_restart(self):
        bridge1 = ClaudeMessagingProtocol(
            bridge_socket_path=self.sock_path,
            session_name=self.session_name,
            db_path=self.db_path
        )
        bridge1.response_store["out_persist_1"] = {
            "direction": "outbound",
            "status": "dispatched",
            "content": "outbound message test",
            "timestamp": time.time()
        }
        bridge1.cleanup_socket()

        bridge2 = ClaudeMessagingProtocol(
            bridge_socket_path=self.sock_path,
            session_name=self.session_name,
            db_path=self.db_path
        )
        self.assertIn("out_persist_1", bridge2.response_store)
        self.assertEqual(bridge2.response_store["out_persist_1"]["content"], "outbound message test")
        self.assertEqual(bridge2.get_unread_messages(), {})
        bridge2.cleanup_socket()

    async def test_session_isolation(self):
        bridge_a = ClaudeMessagingProtocol(
            bridge_socket_path=os.path.join(self.tmp_dir.name, "a.sock"),
            session_name="session-a",
            db_path=self.db_path
        )
        bridge_a.response_store["msg_a"] = {
            "direction": "inbound",
            "read": False,
            "content": "message for A",
            "timestamp": time.time()
        }
        bridge_a.cleanup_socket()

        bridge_b = ClaudeMessagingProtocol(
            bridge_socket_path=os.path.join(self.tmp_dir.name, "b.sock"),
            session_name="session-b",
            db_path=self.db_path
        )
        self.assertNotIn("msg_a", bridge_b.response_store)
        self.assertEqual(bridge_b.get_unread_messages(), {})
        bridge_b.cleanup_socket()

    async def test_rename_session_migrates_persisted_messages(self):
        bridge = ClaudeMessagingProtocol(
            bridge_socket_path=self.sock_path,
            session_name="old-name",
            db_path=self.db_path
        )
        bridge.response_store["renamed_msg"] = {
            "direction": "inbound",
            "read": False,
            "content": "rename test",
            "timestamp": time.time()
        }
        bridge.rename_session_in_store("old-name", "new-name")
        bridge.cleanup_socket()

        bridge_restarted = ClaudeMessagingProtocol(
            bridge_socket_path=self.sock_path,
            session_name="new-name",
            db_path=self.db_path
        )
        self.assertIn("renamed_msg", bridge_restarted.response_store)
        self.assertEqual(bridge_restarted.response_store["renamed_msg"]["content"], "rename test")
        bridge_restarted.cleanup_socket()

    async def test_pruning_keeps_unread_and_removes_old_read(self):
        from claude_chatter.storage import MessageStore
        store = MessageStore(self.db_path)
        old_time = time.time() - (40 * 86400)

        store.save_message("old_unread", {
            "direction": "inbound",
            "read": False,
            "content": "important unread",
            "timestamp": old_time
        }, session_name="prune-sess")

        store.save_message("old_read", {
            "direction": "inbound",
            "read": True,
            "content": "old already read",
            "timestamp": old_time
        }, session_name="prune-sess")

        store.save_message("old_outbound", {
            "direction": "outbound",
            "content": "old sent",
            "timestamp": old_time
        }, session_name="prune-sess")

        store.prune(max_age_days=30)
        loaded = store.load_session_messages("prune-sess")
        self.assertIn("old_unread", loaded)
        self.assertNotIn("old_read", loaded)
        self.assertNotIn("old_outbound", loaded)
        store.close()

    async def test_duplicate_msg_id_across_sessions_does_not_overwrite(self):
        from claude_chatter.storage import MessageStore
        store = MessageStore(self.db_path)
        store.save_message("dup", {"direction": "inbound", "read": False, "content": "for A", "timestamp": 1.0}, "sess-a")
        store.save_message("dup", {"direction": "inbound", "read": False, "content": "for B", "timestamp": 2.0}, "sess-b")

        sess_a_msgs = store.load_session_messages("sess-a")
        sess_b_msgs = store.load_session_messages("sess-b")
        self.assertIn("dup", sess_a_msgs)
        self.assertEqual(sess_a_msgs["dup"]["content"], "for A")
        self.assertIn("dup", sess_b_msgs)
        self.assertEqual(sess_b_msgs["dup"]["content"], "for B")
        store.close()

    async def test_migration_from_single_pk_schema(self):
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        with conn:
            conn.execute(
                """
                CREATE TABLE messages (
                    msg_id TEXT PRIMARY KEY,
                    session_name TEXT NOT NULL,
                    cwd_digest TEXT,
                    direction TEXT NOT NULL,
                    read INTEGER NOT NULL DEFAULT 0,
                    sender TEXT,
                    target_session TEXT,
                    target_pid INTEGER,
                    content TEXT,
                    status TEXT,
                    timestamp REAL NOT NULL,
                    entry_json TEXT NOT NULL
                )
                """
            )
            entry = {"direction": "inbound", "read": False, "content": "migrated msg", "timestamp": 1.0}
            conn.execute(
                """
                INSERT INTO messages VALUES (
                    'old_msg_1', 'sess_old', 'digest1', 'inbound', 0,
                    'peer', NULL, NULL, 'migrated msg', 'received', 1.0, ?
                )
                """,
                (json.dumps(entry),)
            )
        conn.close()

        from claude_chatter.storage import MessageStore
        store = MessageStore(self.db_path)
        msgs = store.load_session_messages("sess_old")
        self.assertIn("old_msg_1", msgs)
        self.assertEqual(msgs["old_msg_1"]["content"], "migrated msg")

        store.save_message("new_msg_2", {"direction": "inbound", "read": False, "content": "after migration", "timestamp": 2.0}, "sess_old")
        msgs_after = store.load_session_messages("sess_old")
        self.assertIn("new_msg_2", msgs_after)
        store.close()

    async def test_bare_cli_inbox_and_history_with_live_bridge(self):
        bridge = ClaudeMessagingProtocol(
            bridge_socket_path=self.sock_path,
            db_path=self.db_path
        )
        bridge.register_session_descriptor(session_name=bridge.session_name)
        bridge.response_store["m1"] = {
            "direction": "inbound",
            "read": False,
            "content": "live bridge msg",
            "timestamp": time.time()
        }
        try:
            cmd_inbox = [
                sys.executable, "-m", "claude_chatter.cli",
                "--inbox", "--db", self.db_path
            ]
            res_inbox = subprocess.run(cmd_inbox, capture_output=True, text=True, check=True)
            data_inbox = json.loads(res_inbox.stdout)
            self.assertIn("m1", data_inbox)
            self.assertEqual(data_inbox["m1"]["content"], "live bridge msg")

            cmd_hist = [
                sys.executable, "-m", "claude_chatter.cli",
                "--history", "--db", self.db_path
            ]
            res_hist = subprocess.run(cmd_hist, capture_output=True, text=True, check=True)
            data_hist = json.loads(res_hist.stdout)
            self.assertIn("m1", data_hist)
        finally:
            bridge.cleanup_session_descriptor()
            bridge.cleanup_socket()

    async def test_cli_inbox_and_history(self):
        bridge = ClaudeMessagingProtocol(
            bridge_socket_path=self.sock_path,
            session_name=self.session_name,
            db_path=self.db_path
        )
        bridge.response_store["cli_in_1"] = {
            "direction": "inbound",
            "read": False,
            "content": "cli test payload",
            "timestamp": time.time()
        }
        bridge.cleanup_socket()

        cmd_inbox = [
            sys.executable, "-m", "claude_chatter.cli",
            "--inbox", "--name", self.session_name, "--db", self.db_path
        ]
        res_inbox = subprocess.run(cmd_inbox, capture_output=True, text=True, check=True)
        data_inbox = json.loads(res_inbox.stdout)
        self.assertIn("cli_in_1", data_inbox)

        cmd_hist = [
            sys.executable, "-m", "claude_chatter.cli",
            "--history", "--name", self.session_name, "--db", self.db_path
        ]
        res_hist = subprocess.run(cmd_hist, capture_output=True, text=True, check=True)
        data_hist = json.loads(res_hist.stdout)
        self.assertIn("cli_in_1", data_hist)

    async def test_host_reload_waits_for_dying_process(self):
        proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(0.2)"])
        sessions_dir = os.path.join(os.path.expanduser("~"), ".claude", "sessions")
        os.makedirs(sessions_dir, exist_ok=True)
        fake_desc = os.path.join(sessions_dir, f"{proc.pid}.json")
        desc_data = {
            "name": "dying-reload-sess",
            "pid": proc.pid,
            "cwd": os.getcwd()
        }
        with open(fake_desc, "w", encoding="utf-8") as f:
            json.dump(desc_data, f)

        bridge = ClaudeMessagingProtocol(
            bridge_socket_path=self.sock_path,
            db_path=self.db_path
        )
        try:
            in_use = bridge._session_name_in_use("dying-reload-sess", wait_if_same_cwd=True, max_wait=1.5, poll_interval=0.05)
            self.assertFalse(in_use)
        finally:
            proc.wait()
            if os.path.exists(fake_desc):
                os.remove(fake_desc)
            bridge.cleanup_socket()

    async def test_bare_cli_inbox_with_no_persisted_name_creates_no_files(self):
        empty_dir = os.path.join(self.tmp_dir.name, "empty_workspace")
        os.makedirs(empty_dir, exist_ok=True)
        names_dir = os.path.join(os.path.expanduser("~"), ".claude", "chatter-session-names")
        files_before = set(os.listdir(names_dir)) if os.path.exists(names_dir) else set()

        cmd = [sys.executable, "-m", "claude_chatter.cli", "--inbox", "--db", self.db_path]
        res = subprocess.run(cmd, cwd=empty_dir, capture_output=True, text=True, check=True)
        self.assertEqual(res.stdout.strip(), "{}")

        files_after = set(os.listdir(names_dir)) if os.path.exists(names_dir) else set()
        self.assertEqual(files_before, files_after)

    async def test_bare_cli_inbox_with_env_named_bridge(self):
        work_dir = os.path.join(self.tmp_dir.name, "env_workspace")
        os.makedirs(work_dir, exist_ok=True)
        work_digest = hashlib.sha1(work_dir.encode("utf-8")).hexdigest()[:8]

        from claude_chatter.storage import MessageStore
        store = MessageStore(self.db_path)
        store.ensure_session("env-named-sess", cwd_digest=work_digest)
        store.save_message("m_env_1", {"direction": "inbound", "read": False, "content": "from env bridge", "timestamp": time.time()}, "env-named-sess", cwd_digest=work_digest)
        store.close()

        names_dir = os.path.join(os.path.expanduser("~"), ".claude", "chatter-session-names")
        os.makedirs(names_dir, exist_ok=True)
        name_file = os.path.join(names_dir, f"{work_digest}.name")
        with open(name_file, "w", encoding="utf-8") as f:
            f.write("env-named-sess")

        try:
            cmd = [sys.executable, "-m", "claude_chatter.cli", "--inbox", "--db", self.db_path]
            clean_env = os.environ.copy()
            clean_env.pop("CHATTER_SESSION_NAME", None)
            res = subprocess.run(cmd, cwd=work_dir, capture_output=True, text=True, check=True, env=clean_env)
            data = json.loads(res.stdout)
            self.assertIn("m_env_1", data)
            self.assertEqual(data["m_env_1"]["content"], "from env bridge")

            # When .name file is deleted, it resolves via SQLite sessions table's cwd_digest
            os.remove(name_file)
            res_db = subprocess.run(cmd, cwd=work_dir, capture_output=True, text=True, check=True, env=clean_env)
            data_db = json.loads(res_db.stdout)
            self.assertIn("m_env_1", data_db)
            self.assertEqual(data_db["m_env_1"]["content"], "from env bridge")
        finally:
            if os.path.exists(name_file):
                os.remove(name_file)

    async def test_cli_readonly_inbox_leaves_identity_and_db_unchanged(self):
        ws_dir = os.path.join(self.tmp_dir.name, "readonly_cli_ws")
        os.makedirs(ws_dir, exist_ok=True)
        ws_digest = hashlib.sha1(ws_dir.encode("utf-8")).hexdigest()[:8]

        names_dir = os.path.join(os.path.expanduser("~"), ".claude", "chatter-session-names")
        os.makedirs(names_dir, exist_ok=True)
        name_file = os.path.join(names_dir, f"{ws_digest}.name")
        id_file = os.path.join(names_dir, f"{ws_digest}.id")

        local_name = "local-bridge-sess"
        local_id = str(uuid.uuid4())
        with open(name_file, "w", encoding="utf-8") as f:
            f.write(local_name)
        with open(id_file, "w", encoding="utf-8") as f:
            f.write(local_id)

        from claude_chatter.storage import MessageStore
        store = MessageStore(self.db_path)
        store.ensure_session(local_name, cwd_digest=ws_digest, session_id=local_id)
        store.close()

        try:
            cmd = [
                sys.executable, "-m", "claude_chatter.cli",
                "--inbox", "--name", "other-external-sess", "--db", self.db_path
            ]
            res = subprocess.run(cmd, cwd=ws_dir, capture_output=True, text=True, check=True)
            self.assertEqual(res.stdout.strip(), "{}")

            with open(name_file, "r", encoding="utf-8") as f:
                self.assertEqual(f.read().strip(), local_name)
            with open(id_file, "r", encoding="utf-8") as f:
                self.assertEqual(f.read().strip(), local_id)

            store_check = MessageStore(self.db_path)
            self.assertIsNone(store_check.resolve_session_id("other-external-sess"))
            with store_check._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT current_name FROM sessions WHERE current_name = ?", ("other-external-sess",))
                self.assertIsNone(cursor.fetchone())
                cursor.execute("SELECT alias_name FROM session_aliases WHERE alias_name = ?", ("other-external-sess",))
                self.assertIsNone(cursor.fetchone())
            store_check.close()
        finally:
            if os.path.exists(name_file):
                os.remove(name_file)
            if os.path.exists(id_file):
                os.remove(id_file)

    async def test_cli_list_returns_promptly_with_live_same_project_holder(self):
        proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(5)"])
        sessions_dir = os.path.join(os.path.expanduser("~"), ".claude", "sessions")
        os.makedirs(sessions_dir, exist_ok=True)
        fake_desc = os.path.join(sessions_dir, f"{proc.pid}.json")

        cur_cwd = os.getcwd()
        cur_digest = hashlib.sha1(cur_cwd.encode("utf-8")).hexdigest()[:8]
        desc_data = {
            "name": "holder-sess",
            "pid": proc.pid,
            "cwd": cur_cwd,
            "projectPath": cur_cwd,
            "cwdDigest": cur_digest
        }
        with open(fake_desc, "w", encoding="utf-8") as f:
            json.dump(desc_data, f)

        names_dir = os.path.join(os.path.expanduser("~"), ".claude", "chatter-session-names")
        os.makedirs(names_dir, exist_ok=True)
        name_file = os.path.join(names_dir, f"{cur_digest}.name")
        with open(name_file, "w", encoding="utf-8") as f:
            f.write("holder-sess")

        try:
            t0 = time.time()
            cli_proto = ClaudeMessagingProtocol(
                bridge_socket_path=None,
                enable_storage=False,
                as_bridge=False
            )
            t1 = time.time()
            self.assertLess(t1 - t0, 0.5)
            self.assertNotEqual(cli_proto.session_name, "holder-sess")

            t_cmd0 = time.time()
            cmd = [sys.executable, "-m", "claude_chatter.cli", "--list"]
            res = subprocess.run(cmd, capture_output=True, text=True, check=True)
            self.assertEqual(res.returncode, 0)
            t_cmd1 = time.time()
            self.assertLess(t_cmd1 - t_cmd0, 0.8)
        finally:
            proc.kill()
            proc.wait()
            if os.path.exists(fake_desc):
                os.remove(fake_desc)
            if os.path.exists(name_file):
                os.remove(name_file)

    async def test_readonly_store_and_cli_leaves_main_db_unmodified_and_works_on_readonly_db(self):
        ro_dir = os.path.join(self.tmp_dir.name, "ro_test_dir")
        os.makedirs(ro_dir, exist_ok=True)
        ro_db = os.path.join(ro_dir, "ro_messages.db")

        from claude_chatter.storage import MessageStore
        init_store = MessageStore(ro_db)
        init_store.ensure_session("ro-sess", session_id="ro-id-1")
        init_store.save_message(
            "m_ro_1",
            {"direction": "inbound", "read": False, "content": "ro test payload", "timestamp": time.time()},
            "ro-sess",
            session_id="ro-id-1"
        )
        init_store.close()

        with open(ro_db, "rb") as f:
            original_hash = hashlib.sha256(f.read()).hexdigest()

        os.chmod(ro_db, 0o444)

        store = MessageStore(ro_db, read_only=True)
        self.assertFalse(store.is_disabled())
        msgs = store.load_session_messages("ro-sess")
        self.assertIn("m_ro_1", msgs)
        store.close()

        with open(ro_db, "rb") as f:
            self.assertEqual(hashlib.sha256(f.read()).hexdigest(), original_hash)

        wal_file = ro_db + "-wal"
        if os.path.exists(wal_file):
            self.assertEqual(os.path.getsize(wal_file), 0)

        cmd = [sys.executable, "-m", "claude_chatter.cli", "--inbox", "--name", "ro-sess", "--db", ro_db]
        res = subprocess.run(cmd, capture_output=True, text=True, check=True)
        data = json.loads(res.stdout)
        self.assertIn("m_ro_1", data)

        with open(ro_db, "rb") as f:
            self.assertEqual(hashlib.sha256(f.read()).hexdigest(), original_hash)

        if os.path.exists(wal_file):
            self.assertEqual(os.path.getsize(wal_file), 0)

    async def test_resolve_session_name_with_multiple_live_descriptors(self):
        work_dir = os.path.join(self.tmp_dir.name, "multi_live_ws")
        os.makedirs(work_dir, exist_ok=True)
        work_digest = hashlib.sha1(work_dir.encode("utf-8")).hexdigest()[:8]

        sessions_dir = os.path.join(os.path.expanduser("~"), ".claude", "sessions")
        os.makedirs(sessions_dir, exist_ok=True)

        p1 = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(10)"])
        p2 = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(10)"])

        desc1_path = os.path.join(sessions_dir, "bridge-sess-alpha.json")
        desc2_path = os.path.join(sessions_dir, "bridge-sess-beta.json")

        with open(desc1_path, "w", encoding="utf-8") as f:
            json.dump({
                "name": "sess-alpha",
                "pid": p1.pid,
                "cwdDigest": work_digest,
                "cwd": work_dir,
                "projectPath": work_dir
            }, f)

        with open(desc2_path, "w", encoding="utf-8") as f:
            json.dump({
                "name": "sess-beta",
                "pid": p2.pid,
                "cwdDigest": work_digest,
                "cwd": work_dir,
                "projectPath": work_dir
            }, f)

        try:
            stderr_buf = io.StringIO()
            with mock.patch("sys.stderr", stderr_buf):
                res = ClaudeMessagingProtocol.resolve_session_name(project_path=work_dir, db_path=self.db_path)
            self.assertEqual(res, "")
            err_msg = stderr_buf.getvalue()
            self.assertIn("Multiple live bridge sessions found for this workspace", err_msg)
            self.assertIn("sess-alpha", err_msg)
            self.assertIn("sess-beta", err_msg)

            os.remove(desc2_path)
            res_single = ClaudeMessagingProtocol.resolve_session_name(project_path=work_dir, db_path=self.db_path)
            self.assertEqual(res_single, "sess-alpha")
        finally:
            p1.kill()
            p2.kill()
            p1.wait()
            p2.wait()
            if os.path.exists(desc1_path):
                os.remove(desc1_path)
            if os.path.exists(desc2_path):
                os.remove(desc2_path)

    async def test_concurrent_bridges_in_same_workspace_keep_separate_identities_and_inboxes(self):
        workspace = tempfile.mkdtemp(prefix="concurrent_ws_")
        old_cwd = os.getcwd()
        os.chdir(workspace)
        holder_proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(15)"])
        sessions_dir = os.path.join(os.path.expanduser("~"), ".claude", "sessions")
        os.makedirs(sessions_dir, exist_ok=True)
        holder_desc = os.path.join(sessions_dir, f"{holder_proc.pid}.json")

        digest = hashlib.sha1(workspace.encode("utf-8")).hexdigest()[:8]
        first_name = "holder-ws-app"
        with open(holder_desc, "w", encoding="utf-8") as f:
            json.dump({
                "pid": holder_proc.pid,
                "name": first_name,
                "cwd": workspace,
                "projectPath": workspace,
                "cwdDigest": digest
            }, f)

        names_dir = os.path.join(os.path.expanduser("~"), ".claude", "chatter-session-names")
        os.makedirs(names_dir, exist_ok=True)
        with open(os.path.join(names_dir, f"{digest}.name"), "w", encoding="utf-8") as f:
            f.write(first_name)
        first_id = str(uuid.uuid4())
        with open(os.path.join(names_dir, f"{digest}.id"), "w", encoding="utf-8") as f:
            f.write(first_id)

        from claude_chatter.storage import MessageStore
        init_store = MessageStore(self.db_path)
        init_store.ensure_session(first_name, cwd_digest=digest, session_id=first_id)
        init_store.save_message(
            "m_first_1",
            {"direction": "inbound", "read": False, "content": "from first session", "timestamp": time.time()},
            first_name,
            cwd_digest=digest,
            session_id=first_id
        )
        init_store.close()

        sock2 = os.path.join(self.tmp_dir.name, "b2.sock")
        try:
            b2 = ClaudeMessagingProtocol(bridge_socket_path=sock2, db_path=self.db_path, as_bridge=True)
            self.assertTrue(getattr(b2, "_is_clashing", False))
            self.assertNotEqual(b2.session_name, first_name)
            self.assertNotEqual(b2.session_id, first_id)
            self.assertNotIn("m_first_1", b2.response_store)

            check_store = MessageStore(self.db_path, read_only=True)
            with check_store._get_connection() as conn:
                cur = conn.cursor()
                cur.execute("SELECT session_id, current_name FROM sessions WHERE session_id = ?", (first_id,))
                row1 = cur.fetchone()
                self.assertIsNotNone(row1)
                self.assertEqual(row1["current_name"], first_name)

                cur.execute("SELECT session_id, current_name FROM sessions WHERE session_id = ?", (b2.session_id,))
                row2 = cur.fetchone()
                self.assertIsNotNone(row2)
                self.assertEqual(row2["current_name"], b2.session_name)
            check_store.close()
            b2.cleanup_socket()
        finally:
            holder_proc.kill()
            holder_proc.wait()
            os.chdir(old_cwd)
            if os.path.exists(holder_desc):
                os.remove(holder_desc)
            name_file = os.path.join(names_dir, f"{digest}.name")
            id_file = os.path.join(names_dir, f"{digest}.id")
            if os.path.exists(name_file):
                os.remove(name_file)
            if os.path.exists(id_file):
                os.remove(id_file)

    async def test_host_reload_inherits_identity_and_unread_messages(self):
        workspace = tempfile.mkdtemp(prefix="reload_ws_")
        old_cwd = os.getcwd()
        os.chdir(workspace)
        dying_proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(0.2)"])
        sessions_dir = os.path.join(os.path.expanduser("~"), ".claude", "sessions")
        os.makedirs(sessions_dir, exist_ok=True)
        dying_desc = os.path.join(sessions_dir, f"{dying_proc.pid}.json")

        digest = hashlib.sha1(workspace.encode("utf-8")).hexdigest()[:8]
        reload_name = "reload-app"
        with open(dying_desc, "w", encoding="utf-8") as f:
            json.dump({
                "pid": dying_proc.pid,
                "name": reload_name,
                "cwd": workspace,
                "projectPath": workspace,
                "cwdDigest": digest
            }, f)

        names_dir = os.path.join(os.path.expanduser("~"), ".claude", "chatter-session-names")
        os.makedirs(names_dir, exist_ok=True)
        with open(os.path.join(names_dir, f"{digest}.name"), "w", encoding="utf-8") as f:
            f.write(reload_name)
        reload_id = str(uuid.uuid4())
        with open(os.path.join(names_dir, f"{digest}.id"), "w", encoding="utf-8") as f:
            f.write(reload_id)

        from claude_chatter.storage import MessageStore
        init_store = MessageStore(self.db_path)
        init_store.ensure_session(reload_name, cwd_digest=digest, session_id=reload_id)
        init_store.save_message(
            "m_reload_1",
            {"direction": "inbound", "read": False, "content": "persisted unread msg", "timestamp": time.time()},
            reload_name,
            cwd_digest=digest,
            session_id=reload_id
        )
        init_store.close()

        sock_reload = os.path.join(self.tmp_dir.name, "reload.sock")
        try:
            b_reload = ClaudeMessagingProtocol(bridge_socket_path=sock_reload, db_path=self.db_path, as_bridge=True)
            self.assertFalse(getattr(b_reload, "_is_clashing", False))
            self.assertEqual(b_reload.session_name, reload_name)
            self.assertEqual(b_reload.session_id, reload_id)
            self.assertIn("m_reload_1", b_reload.response_store)
            self.assertEqual(b_reload.response_store["m_reload_1"]["content"], "persisted unread msg")
            b_reload.cleanup_socket()
        finally:
            dying_proc.wait()
            os.chdir(old_cwd)
            if os.path.exists(dying_desc):
                os.remove(dying_desc)
            name_file = os.path.join(names_dir, f"{digest}.name")
            id_file = os.path.join(names_dir, f"{digest}.id")
            if os.path.exists(name_file):
                os.remove(name_file)
            if os.path.exists(id_file):
                os.remove(id_file)

    async def test_periodic_prune_on_save_message(self):
        from claude_chatter.storage import MessageStore
        store = MessageStore(self.db_path)
        old_time = time.time() - (40 * 86400)
        store.save_message("old_prune_msg", {"direction": "inbound", "read": True, "content": "old", "timestamp": old_time}, "prune-sess")
        store._last_prune_time = time.time() - 4000
        store.save_message("new_save_msg", {"direction": "inbound", "read": False, "content": "new", "timestamp": time.time()}, "prune-sess")

        msgs = store.load_session_messages("prune-sess")
        self.assertNotIn("old_prune_msg", msgs)
        self.assertIn("new_save_msg", msgs)
        store.close()

    async def test_rename_restart_history_and_unread_intact(self):
        bridge1 = ClaudeMessagingProtocol(
            bridge_socket_path=self.sock_path,
            session_name="sess-1",
            db_path=self.db_path
        )
        now = time.time()
        bridge1.response_store["in_1"] = {
            "direction": "inbound",
            "read": False,
            "content": "unread inbound",
            "timestamp": now
        }
        bridge1.response_store["out_1"] = {
            "direction": "outbound",
            "content": "dispatched outbound",
            "timestamp": now + 1.0
        }
        bridge1.rename_session_in_store("sess-1", "sess-2")
        bridge1.cleanup_socket()

        bridge2 = ClaudeMessagingProtocol(
            bridge_socket_path=self.sock_path,
            session_name="sess-2",
            db_path=self.db_path
        )
        self.assertIn("in_1", bridge2.response_store)
        self.assertIn("out_1", bridge2.response_store)
        unread = bridge2.get_unread_messages(mark_read=False)
        self.assertIn("in_1", unread)
        self.assertEqual(unread["in_1"]["content"], "unread inbound")
        bridge2.cleanup_socket()

    async def test_rename_twice(self):
        bridge = ClaudeMessagingProtocol(
            bridge_socket_path=self.sock_path,
            session_name="name-a",
            db_path=self.db_path
        )
        bridge.response_store["msg_a"] = {"direction": "inbound", "read": False, "content": "stage a", "timestamp": 1.0}
        bridge.rename_session_in_store("name-a", "name-b")
        bridge.response_store["msg_b"] = {"direction": "inbound", "read": False, "content": "stage b", "timestamp": 2.0}
        bridge.rename_session_in_store("name-b", "name-c")
        bridge.response_store["msg_c"] = {"direction": "inbound", "read": False, "content": "stage c", "timestamp": 3.0}

        msgs_a = bridge.store.load_session_messages("name-a")
        msgs_b = bridge.store.load_session_messages("name-b")
        msgs_c = bridge.store.load_session_messages("name-c")

        for msgs in (msgs_a, msgs_b, msgs_c):
            self.assertEqual(set(msgs.keys()), {"msg_a", "msg_b", "msg_c"})
        bridge.cleanup_socket()

    async def test_query_by_old_and_new_name(self):
        from claude_chatter.storage import MessageStore
        store = MessageStore(self.db_path)
        store.save_message("m_alpha", {"direction": "inbound", "read": False, "content": "alpha msg", "timestamp": 1.0}, "sess-alpha")
        store.rename_session("sess-alpha", "sess-beta")
        store.save_message("m_beta", {"direction": "inbound", "read": False, "content": "beta msg", "timestamp": 2.0}, "sess-beta")

        by_old = store.load_session_messages("sess-alpha")
        by_new = store.load_session_messages("sess-beta")
        self.assertEqual(set(by_old.keys()), {"m_alpha", "m_beta"})
        self.assertEqual(set(by_new.keys()), {"m_alpha", "m_beta"})
        store.close()

    async def test_rename_onto_existing_name_with_duplicate_msg_id(self):
        from claude_chatter.storage import MessageStore
        store = MessageStore(self.db_path)
        store.save_message("dup_id", {"direction": "inbound", "read": False, "content": "from target", "timestamp": 1.0}, "target-sess")
        store.save_message("target_unique", {"direction": "inbound", "read": False, "content": "target only", "timestamp": 2.0}, "target-sess")

        store.save_message("dup_id", {"direction": "inbound", "read": False, "content": "from source", "timestamp": 3.0}, "source-sess")
        store.save_message("source_unique", {"direction": "inbound", "read": False, "content": "source only", "timestamp": 4.0}, "source-sess")

        store.rename_session("source-sess", "target-sess")
        merged = store.load_session_messages("target-sess")
        self.assertIn("dup_id", merged)
        self.assertIn("target_unique", merged)
        self.assertIn("source_unique", merged)
        store.close()

    async def test_resolve_session_name_ignores_interactive_descriptors_and_resolves_via_file_or_db(self):
        work_dir = os.path.join(self.tmp_dir.name, "resolve_test_dir")
        os.makedirs(work_dir, exist_ok=True)
        work_digest = hashlib.sha1(work_dir.encode("utf-8")).hexdigest()[:8]

        sessions_dir = os.path.join(os.path.expanduser("~"), ".claude", "sessions")
        os.makedirs(sessions_dir, exist_ok=True)
        fake_interactive = os.path.join(sessions_dir, f"{os.getpid()}_interactive.json")
        desc = {
            "name": "interactive-claude-session",
            "pid": os.getpid(),
            "kind": "interactive",
            "cwd": work_dir
        }
        with open(fake_interactive, "w", encoding="utf-8") as f:
            json.dump(desc, f)

        try:
            resolved = ClaudeMessagingProtocol.resolve_session_name(project_path=work_dir, db_path=self.db_path)
            self.assertEqual(resolved, "")

            names_dir = os.path.join(os.path.expanduser("~"), ".claude", "chatter-session-names")
            os.makedirs(names_dir, exist_ok=True)
            name_file = os.path.join(names_dir, f"{work_digest}.name")
            with open(name_file, "w", encoding="utf-8") as f:
                f.write("resolved-from-file")

            resolved = ClaudeMessagingProtocol.resolve_session_name(project_path=work_dir, db_path=self.db_path)
            self.assertEqual(resolved, "resolved-from-file")

            os.remove(name_file)
            from claude_chatter.storage import MessageStore
            store = MessageStore(self.db_path)
            store.ensure_session("resolved-from-db", cwd_digest=work_digest)
            store.close()

            resolved = ClaudeMessagingProtocol.resolve_session_name(project_path=work_dir, db_path=self.db_path)
            self.assertEqual(resolved, "resolved-from-db")
        finally:
            if os.path.exists(fake_interactive):
                os.remove(fake_interactive)
            name_file = os.path.join(os.path.expanduser("~"), ".claude", "chatter-session-names", f"{work_digest}.name")
            if os.path.exists(name_file):
                os.remove(name_file)

    async def test_alias_reuse_does_not_adopt_stranger_history(self):
        from claude_chatter.storage import MessageStore
        store = MessageStore(self.db_path)
        sid_1 = store.ensure_session("work", cwd_digest="digest_1")
        store.save_message("msg_work_1", {
            "direction": "inbound",
            "read": False,
            "content": "confidential message",
            "timestamp": time.time()
        }, "work", cwd_digest="digest_1", session_id=sid_1)

        store.rename_session("work", "work2", session_id=sid_1)
        self.assertIn("msg_work_1", store.load_session_messages("work2"))

        sid_2 = store.ensure_session("work", cwd_digest="digest_2")
        self.assertNotEqual(sid_1, sid_2)

        new_msgs = store.load_session_messages(sid_2)
        self.assertEqual(new_msgs, {})

        work_msgs = store.load_session_messages("work")
        self.assertEqual(work_msgs, {})

        work2_msgs = store.load_session_messages("work2")
        self.assertIn("msg_work_1", work2_msgs)
        store.close()

    async def test_atomic_migration_rollback_on_failure(self):
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        with conn:
            conn.execute(
                """
                CREATE TABLE messages (
                    msg_id TEXT PRIMARY KEY,
                    session_name TEXT NOT NULL,
                    cwd_digest TEXT,
                    direction TEXT NOT NULL,
                    read INTEGER NOT NULL DEFAULT 0,
                    sender TEXT,
                    target_session TEXT,
                    target_pid INTEGER,
                    content TEXT,
                    status TEXT,
                    timestamp REAL NOT NULL,
                    entry_json TEXT NOT NULL
                )
                """
            )
            entry = {"direction": "inbound", "read": False, "content": "v1 msg", "timestamp": 1.0}
            conn.execute(
                """
                INSERT INTO messages VALUES (
                    'm1', 'sess1', 'dig1', 'inbound', 0,
                    'peer', NULL, NULL, 'v1 msg', 'received', 1.0, ?
                )
                """,
                (json.dumps(entry),)
            )
        conn.close()

        from unittest import mock
        from claude_chatter.storage import MessageStore, SCHEMA_STATEMENTS

        with mock.patch("claude_chatter.storage.SCHEMA_STATEMENTS", [
            SCHEMA_STATEMENTS[0],
            SCHEMA_STATEMENTS[1],
            "CREATE TABLE messages (invalid syntax...)",
        ]):
            store = MessageStore(self.db_path)
            self.assertTrue(store.is_disabled())
            self.assertIn("syntax error", store.last_error.lower())

        raw_conn = sqlite3.connect(self.db_path)
        tables = [r[0] for r in raw_conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        self.assertIn("messages", tables)
        self.assertNotIn("messages_old", tables)
        raw_conn.close()

    async def test_interrupted_migration_startup_recovery(self):
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        with conn:
            conn.execute(
                """
                CREATE TABLE messages_old (
                    msg_id TEXT PRIMARY KEY,
                    session_name TEXT NOT NULL,
                    cwd_digest TEXT,
                    direction TEXT NOT NULL,
                    read INTEGER NOT NULL DEFAULT 0,
                    sender TEXT,
                    target_session TEXT,
                    target_pid INTEGER,
                    content TEXT,
                    status TEXT,
                    timestamp REAL NOT NULL,
                    entry_json TEXT NOT NULL
                )
                """
            )
            entry = {"direction": "inbound", "read": False, "content": "stranded data", "timestamp": 1.0}
            conn.execute(
                """
                INSERT INTO messages_old VALUES (
                    'm_stranded_1', 'stranded_sess', 'dig_str', 'inbound', 0,
                    'peer', NULL, NULL, 'stranded data', 'received', 1.0, ?
                )
                """,
                (json.dumps(entry),)
            )
        conn.close()

        from claude_chatter.storage import MessageStore
        store = MessageStore(self.db_path)
        self.assertFalse(store.is_disabled())
        msgs = store.load_session_messages("stranded_sess")
        self.assertIn("m_stranded_1", msgs)
        self.assertEqual(msgs["m_stranded_1"]["content"], "stranded data")

        raw_conn = sqlite3.connect(self.db_path)
        tables = [r[0] for r in raw_conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        self.assertNotIn("messages_old", tables)
        raw_conn.close()
        store.close()

    async def test_host_reload_waits_for_plugin_bridge_matching_project_path_or_cwd_digest(self):
        proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(0.2)"])
        sessions_dir = os.path.join(os.path.expanduser("~"), ".claude", "sessions")
        os.makedirs(sessions_dir, exist_ok=True)
        fake_desc = os.path.join(sessions_dir, f"{proc.pid}.json")

        bridge = ClaudeMessagingProtocol(
            bridge_socket_path=self.sock_path,
            db_path=self.db_path
        )
        desc_data = {
            "name": "dying-plugin-sess",
            "pid": proc.pid,
            "cwd": "/some/plugin/install/dir",
            "projectPath": bridge._project_path(),
            "cwdDigest": bridge._cwd_digest()
        }
        with open(fake_desc, "w", encoding="utf-8") as f:
            json.dump(desc_data, f)

        try:
            in_use = bridge._session_name_in_use("dying-plugin-sess", wait_if_same_cwd=True, max_wait=1.5, poll_interval=0.05)
            self.assertFalse(in_use)
        finally:
            proc.wait()
            if os.path.exists(fake_desc):
                os.remove(fake_desc)
            bridge.cleanup_socket()

    async def test_rename_retains_renamed_copy_on_duplicate_msg_id(self):
        from claude_chatter.storage import MessageStore
        store = MessageStore(self.db_path)
        store.save_message("shared_id", {
            "direction": "inbound",
            "read": False,
            "content": "target copy",
            "timestamp": 1.0
        }, "target-session")

        store.save_message("shared_id", {
            "direction": "inbound",
            "read": False,
            "content": "renamed source copy",
            "timestamp": 2.0
        }, "source-session")

        store.rename_session("source-session", "target-session")
        merged = store.load_session_messages("target-session")
        self.assertIn("shared_id", merged)
        self.assertEqual(merged["shared_id"]["content"], "renamed source copy")
        store.close()


class TestBridgeIPCCommunication(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp_dir.name, "test_ipc.db")
        self.prev_db = os.environ.get("CHATTER_DB_PATH")
        os.environ["CHATTER_DB_PATH"] = self.db_path
        self.socket_path = os.path.join(self.tmp_dir.name, "test_ipc.sock")
        self.bridge = ClaudeMessagingProtocol(bridge_socket_path=self.socket_path, db_path=self.db_path)

    async def asyncTearDown(self):
        self.bridge.cleanup_session_descriptor()
        self.bridge.cleanup_socket()
        if self.prev_db is not None:
            os.environ["CHATTER_DB_PATH"] = self.prev_db
        else:
            os.environ.pop("CHATTER_DB_PATH", None)
        self.tmp_dir.cleanup()

    async def test_inbound_listener_and_buffering(self):
        """Verify inbound Unix socket listener receives and buffers peer frames."""
        server_task = asyncio.create_task(self.bridge.start_bridge_listener())
        await asyncio.sleep(0.1)

        self.assertTrue(os.path.exists(self.socket_path))

        reader, writer = await asyncio.open_unix_connection(self.socket_path)

        # Send auth frame
        auth_frame = {"type": "auth", "peerToken": "test_token_123"}
        writer.write((json.dumps(auth_frame) + "\n").encode("utf-8"))

        # Send message frame
        msg_id = "test_msg_suite_99"
        msg_frame = {
            "type": "user",
            "message": {"role": "user", "content": "Suite Test Payload"},
            "msg_id": msg_id,
            "sender": "suite_agent"
        }
        writer.write((json.dumps(msg_frame) + "\n").encode("utf-8"))
        await writer.drain()

        # The listener must not reply: Claude Code's sender treats any reply as a send failure
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(reader.readline(), timeout=0.3)

        writer.close()
        await writer.wait_closed()

        # Verify buffered payload in response_store
        self.assertIn(msg_id, self.bridge.response_store)
        cached = self.bridge.response_store[msg_id]
        self.assertEqual(cached["sender"], "suite_agent")
        self.assertEqual(cached["content"], "Suite Test Payload")

        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass

    async def test_inbound_real_claude_code_frame_format(self):
        """Verify a real Claude Code SendMessage frame (no "sender" field -
        identity embedded as from-name inside a <cross-session-message> tag
        wrapping the actual text, per a live-captured real frame) is parsed
        into a clean sender name and unwrapped content, not "unknown" and a
        content string still containing the wrapper tag."""
        server_task = asyncio.create_task(self.bridge.start_bridge_listener())
        await asyncio.sleep(0.1)

        reader, writer = await asyncio.open_unix_connection(self.socket_path)
        msg_id = "real-format-msg-1"
        real_frame = {
            "msgV": 1,
            "msg_id": msg_id,
            "type": "user",
            "message": {
                "role": "user",
                "content": (
                    '<cross-session-message from="uds:/run/user/1001/cc-socks/123.sock" '
                    'from-name="some-real-session" from-mode="prompting">\n'
                    'hello from a real session\n</cross-session-message>'
                )
            },
            "priority": "next",
            "from": "uds:/run/user/1001/cc-socks/123.sock"
        }
        writer.write((json.dumps(real_frame) + "\n").encode("utf-8"))
        await writer.drain()
        await asyncio.sleep(0.2)
        writer.close()
        await writer.wait_closed()

        cached = self.bridge.response_store[msg_id]
        self.assertEqual(cached["sender"], "some-real-session")
        self.assertEqual(cached["content"], "hello from a real session")

        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass

    async def _post_frames(self, frames):
        """Sends each frame on its own connection, tolerating the server dropping an oversized one."""
        for frame in frames:
            reader, writer = await asyncio.open_unix_connection(self.socket_path)
            try:
                writer.write((json.dumps(frame) + "\n").encode("utf-8"))
                await writer.drain()
                await asyncio.sleep(0.3)
                writer.close()
                await writer.wait_closed()
            except (ConnectionResetError, BrokenPipeError):
                pass

    @staticmethod
    def _frame(msg_id, size):
        return {"msgV": 1, "msg_id": msg_id, "type": "user",
                "message": {"role": "user", "content": "x" * size}}

    async def test_inbound_frames_above_64kib_are_stored_whole(self):
        server_task = asyncio.create_task(self.bridge.start_bridge_listener())
        await asyncio.sleep(0.1)

        await self._post_frames([self._frame("big-100k", 100_000), self._frame("big-900k", 900_000)])

        self.assertEqual(len(self.bridge.response_store["big-100k"]["content"]), 100_000)
        self.assertEqual(len(self.bridge.response_store["big-900k"]["content"]), 900_000)

        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass

    async def test_frame_over_the_cap_is_dropped_and_listener_keeps_serving(self):
        server_task = asyncio.create_task(self.bridge.start_bridge_listener())
        await asyncio.sleep(0.1)

        await self._post_frames([
            self._frame("too-big", ClaudeClient.MAX_FRAME_BYTES + 1000),
            self._frame("after", 100),
        ])

        self.assertNotIn("too-big", self.bridge.response_store)
        self.assertIn("after", self.bridge.response_store)

        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass

    async def test_outbound_send_to_mock_peer(self):
        """Verify outbound frame formatting and delivery over target Unix domain socket."""
        mock_sock_path = os.path.join(self.tmp_dir.name, "mock_target.sock")
        received_frames = []

        async def mock_handler(reader, writer):
            while True:
                line = await reader.readline()
                if not line:
                    break
                received_frames.append(json.loads(line.decode("utf-8").strip()))
            writer.close()
            await writer.wait_closed()

        mock_server = await asyncio.start_unix_server(mock_handler, mock_sock_path)

        self.bridge.active_peers = {
            "mock-peer": {
                "name": "mock-peer",
                "pid": 88888,
                "sessionId": "mock-session-xyz",
                "socket": mock_sock_path,
                "token": "secret_peer_token",
                "cwd": "/tmp"
            }
        }

        async def bypass_verify():
            return self.bridge.active_peers
        self.bridge.verify_and_purge_sessions = bypass_verify

        res = await self.bridge.send_to_claude("mock-peer", "Hello Mock Peer!")
        await asyncio.sleep(0.05)

        self.assertTrue(res["success"])
        self.assertEqual(len(received_frames), 1)
        self.assertEqual(received_frames[0]["type"], "user")
        self.assertEqual(received_frames[0]["msgV"], 1)
        self.assertTrue(received_frames[0]["from"].startswith("uds:"))
        content = received_frames[0]["message"]["content"]
        self.assertIn('from-name="', content)
        self.assertIn("Hello Mock Peer!", content)

        mock_server.close()
        await mock_server.wait_closed()


class PeerDirTestCase(unittest.IsolatedAsyncioTestCase):
    """Sessions dir with two live dummy processes and unix-socket listeners standing in for peers."""

    async def asyncSetUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.sessions_dir = os.path.join(self.tmp_dir.name, "sessions")
        os.makedirs(self.sessions_dir)
        self.procs = [subprocess.Popen(["sleep", "60"]) for _ in range(2)]
        self.pid_a, self.pid_b = self.procs[0].pid, self.procs[1].pid
        self.servers = []
        self.client = ClaudeClient(
            bridge_socket_path=os.path.join(self.tmp_dir.name, "sender.sock"),
            session_name="sender",
            enable_storage=False,
            as_bridge=False,
        )
        self.client.sessions_dir = self.sessions_dir

    async def asyncTearDown(self):
        for server in self.servers:
            server.close()
            await server.wait_closed()
        for proc in self.procs:
            proc.kill()
            proc.wait()
        self.tmp_dir.cleanup()

    def _descriptor(self, filename, pid, name, sock):
        path = os.path.join(self.sessions_dir, filename)
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"pid": pid, "name": name, "messagingSocketPath": sock, "sessionId": f"sid-{pid}"}, f)
        return path

    async def _listen(self, name, received):
        sock = os.path.join(self.tmp_dir.name, f"{name}.sock")

        async def handler(reader, writer):
            line = await reader.readline()
            if line:
                received.append(json.loads(line.decode("utf-8")))
            writer.close()

        self.servers.append(await asyncio.start_unix_server(handler, sock))
        return sock


class TestDuplicateSessionNames(PeerDirTestCase):
    async def test_scan_keeps_every_live_session_sharing_a_name(self):
        self._descriptor(f"{self.pid_a}.json", self.pid_a, "worker", "/sock/a")
        self._descriptor(f"{self.pid_b}.json", self.pid_b, "worker", "/sock/b")

        peers = ClaudeWire.scan_sessions(self.sessions_dir)

        self.assertEqual(
            sorted(peers), sorted([f"worker#{self.pid_a}", f"worker#{self.pid_b}"])
        )
        self.assertEqual({p["socket"] for p in peers.values()}, {"/sock/a", "/sock/b"})
        self.assertEqual({p["name"] for p in peers.values()}, {"worker"})

    async def test_unique_names_stay_keyed_by_plain_name(self):
        self._descriptor(f"{self.pid_a}.json", self.pid_a, "solo", "/sock/a")
        self._descriptor(f"{self.pid_b}.json", self.pid_b, "other", "/sock/b")

        peers = ClaudeWire.scan_sessions(self.sessions_dir)

        self.assertEqual(sorted(peers), ["other", "solo"])

    async def test_named_descriptor_for_same_pid_is_not_a_duplicate(self):
        self._descriptor(f"{self.pid_a}.json", self.pid_a, "worker", "/sock/a")
        self._descriptor("bridge-worker.json", self.pid_a, "worker", "/sock/a")

        peers = ClaudeWire.scan_sessions(self.sessions_dir)

        self.assertEqual(list(peers), ["worker"])

    async def test_verify_and_purge_keeps_duplicates_and_their_files(self):
        file_a = self._descriptor(f"{self.pid_a}.json", self.pid_a, "worker", "/sock/a")
        file_b = self._descriptor(f"{self.pid_b}.json", self.pid_b, "worker", "/sock/b")

        peers = await self.client.verify_and_purge_sessions()

        self.assertEqual(len(peers), 2)
        self.assertTrue(os.path.exists(file_a))
        self.assertTrue(os.path.exists(file_b))

    async def test_send_to_ambiguous_name_is_refused_with_candidates(self):
        received_a, received_b = [], []
        sock_a = await self._listen("a", received_a)
        sock_b = await self._listen("b", received_b)
        self._descriptor(f"{self.pid_a}.json", self.pid_a, "worker", sock_a)
        self._descriptor(f"{self.pid_b}.json", self.pid_b, "worker", sock_b)

        res = await self.client.send_message("worker", "who gets this?")
        await asyncio.sleep(0.05)

        self.assertFalse(res["success"])
        self.assertEqual({c["pid"] for c in res["candidates"]}, {self.pid_a, self.pid_b})
        self.assertEqual(received_a, [])
        self.assertEqual(received_b, [])

    async def test_send_by_pid_reaches_only_that_session(self):
        received_a, received_b = [], []
        sock_a = await self._listen("a", received_a)
        sock_b = await self._listen("b", received_b)
        self._descriptor(f"{self.pid_a}.json", self.pid_a, "worker", sock_a)
        self._descriptor(f"{self.pid_b}.json", self.pid_b, "worker", sock_b)

        res_b = await self.client.send_message(str(self.pid_b), "for b only")
        await asyncio.sleep(0.05)
        self.assertTrue(res_b["success"])
        self.assertEqual(received_a, [])
        self.assertEqual(len(received_b), 1)

        res_a = await self.client.send_message(str(self.pid_a), "for a only")
        await asyncio.sleep(0.05)
        self.assertTrue(res_a["success"])
        self.assertEqual(len(received_a), 1)
        self.assertEqual(len(received_b), 1)

    async def test_send_by_session_id_reaches_only_that_session(self):
        received_a, received_b = [], []
        sock_a = await self._listen("a", received_a)
        sock_b = await self._listen("b", received_b)
        self._descriptor(f"{self.pid_a}.json", self.pid_a, "worker", sock_a)
        self._descriptor(f"{self.pid_b}.json", self.pid_b, "worker", sock_b)

        res = await self.client.send_message(f"sid-{self.pid_a}", "for a only")
        await asyncio.sleep(0.05)

        self.assertTrue(res["success"])
        self.assertEqual(len(received_a), 1)
        self.assertEqual(received_b, [])


class TestTargetResolution(PeerDirTestCase):
    def _peers(self, *names):
        self.client.active_peers = ClaudeWire.index_peers(
            {"name": n, "pid": 1000 + i, "sessionId": f"sid-{i}", "socket": f"/sock/{n}"}
            for i, n in enumerate(names)
        )

    async def test_exact_name_wins_over_longer_names_containing_it(self):
        self._peers("api-worker", "api")

        self.assertEqual(self.client._resolve_session("api")["name"], "api")

    async def test_unique_partial_name_resolves_case_insensitively(self):
        self._peers("api-worker", "db")

        self.assertEqual(self.client._resolve_session("WORK")["name"], "api-worker")

    async def test_ambiguous_partial_name_raises_with_all_candidates(self):
        self._peers("api-worker", "rapid-db", "other")

        with self.assertRaises(AmbiguousSessionError) as ctx:
            self.client._resolve_session("api")

        self.assertEqual({c["name"] for c in ctx.exception.candidates}, {"api-worker", "rapid-db"})

    async def test_catch_all_aliases_do_not_pick_a_session(self):
        self._peers("api-worker", "rapid-db")

        for alias in ("any", "first", "default", "active"):
            self.assertIsNone(self.client._resolve_session(alias), alias)

    async def test_send_to_ambiguous_partial_name_delivers_nothing(self):
        received_a, received_b = [], []
        sock_a = await self._listen("a", received_a)
        sock_b = await self._listen("b", received_b)
        self._descriptor(f"{self.pid_a}.json", self.pid_a, "api-worker", sock_a)
        self._descriptor(f"{self.pid_b}.json", self.pid_b, "rapid-db", sock_b)

        res = await self.client.send_message("api", "which one?")
        await asyncio.sleep(0.05)

        self.assertFalse(res["success"])
        self.assertEqual({c["name"] for c in res["candidates"]}, {"api-worker", "rapid-db"})
        self.assertEqual(received_a, [])
        self.assertEqual(received_b, [])

    async def test_send_to_any_alias_delivers_nothing(self):
        received = []
        sock = await self._listen("a", received)
        self._descriptor(f"{self.pid_a}.json", self.pid_a, "api-worker", sock)

        res = await self.client.send_message("any", "to whoever")
        await asyncio.sleep(0.05)

        self.assertFalse(res["success"])
        self.assertEqual(received, [])


class TestCoreCLI(unittest.TestCase):
    def setUp(self):
        self.script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "claude_chatter", "cli.py"))

    def test_cli_list(self):
        """Verify python3 cli.py --list executes and returns valid JSON."""
        res = subprocess.run(
            [sys.executable, self.script_path, "--list"],
            capture_output=True,
            text=True
        )
        self.assertEqual(res.returncode, 0)
        data = json.loads(res.stdout)
        self.assertIsInstance(data, dict)

    def test_cli_purge(self):
        """Verify python3 cli.py --purge executes successfully."""
        res = subprocess.run(
            [sys.executable, self.script_path, "--purge"],
            capture_output=True,
            text=True
        )
        self.assertEqual(res.returncode, 0)
        self.assertIn("Purge scan completed", res.stdout)

    def test_cli_standalone_daemon(self):
        """Verify python3 cli.py --standalone starts and stops gracefully on SIGTERM."""
        socket_path = f"/tmp/test_standalone_{uuid.uuid4().hex[:8]}.sock"
        proc = subprocess.Popen(
            [sys.executable, self.script_path, "--standalone", "--socket", socket_path, "--name", "test-standalone-daemon"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        try:
            time.sleep(1.0)
            self.assertTrue(os.path.exists(socket_path))
            proc.terminate()
            stdout, stderr = proc.communicate(timeout=3)
            self.assertEqual(proc.returncode, 0)
            self.assertIn("Standalone daemon shut down gracefully", stderr)
            self.assertFalse(os.path.exists(socket_path))
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
            if os.path.exists(socket_path):
                try:
                    os.remove(socket_path)
                except OSError:
                    pass

    def test_cli_standalone_send(self):
        """Verify python3 cli.py --send dispatches to a standalone daemon.
        send_message is fire-and-forget (no auto-reply scaffold, no transcript
        polling) - a real reply, if any, arrives later via a genuine send_message
        call from the target instead."""
        socket_path = f"/tmp/test_pingpong_{uuid.uuid4().hex[:8]}.sock"
        daemon_name = f"suite-daemon-{uuid.uuid4().hex[:6]}"

        proc = subprocess.Popen(
            [sys.executable, self.script_path, "--standalone", "--socket", socket_path, "--name", daemon_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        try:
            time.sleep(1.0)
            self.assertTrue(os.path.exists(socket_path))

            # Send message to the daemon using --send; dispatch should succeed
            # immediately since send_message is fire-and-forget.
            res = subprocess.run(
                [sys.executable, self.script_path, "--send", daemon_name, "hello standalone"],
                capture_output=True,
                text=True,
                timeout=10
            )
            self.assertEqual(res.returncode, 0)
            data = json.loads(res.stdout)
            self.assertTrue(data.get("success"))
            self.assertEqual(data.get("status"), "dispatched")

        finally:
            proc.terminate()
            proc.wait(timeout=3)
            if os.path.exists(socket_path):
                try:
                    os.remove(socket_path)
                except OSError:
                    pass


class TestListJobs(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        job_dir = os.path.join(self.tmp.name, "jobs", "abc12345")
        os.makedirs(job_dir)
        transcript = os.path.join(self.tmp.name, "t.jsonl")
        open(transcript, "w").close()
        with open(os.path.join(job_dir, "state.json"), "w") as f:
            json.dump({
                "state": "done", "detail": "PR #1 open", "output": {"result": "final summary"}, "name": "demo", "daemonShort": "abc12345",
                "updatedAt": "2026-11-02T10:00:00Z", "cwd": "/x",
                "children": [{"id": "1", "kind": "pr", "href": "https://example.test/pr/1"}],
                "respawnFlags": ["--secret"], "providerEnv": {"K": "V"}, "linkScanPath": transcript,
            }, f)
        with open(os.path.join(job_dir, "timeline.jsonl"), "w") as f:
            f.write(json.dumps({"at": "t1", "state": "working", "detail": "a", "text": "long"}) + "\n")
            f.write(json.dumps({"at": "t2", "state": "done", "detail": "b", "text": "long"}) + "\n")
            f.write("not json\n")
        with open(os.path.join(self.tmp.name, "jobs", "bad.json"), "w") as f:
            f.write("{")
        os.makedirs(os.path.join(self.tmp.name, "jobs", "broken"))
        with open(os.path.join(self.tmp.name, "jobs", "broken", "state.json"), "w") as f:
            f.write("{")

    def tearDown(self):
        self.tmp.cleanup()

    def test_lists_finished_job_without_sensitive_fields(self):
        from claude_chatter.jobs import list_jobs
        jobs = list_jobs(claude_dir=self.tmp.name)
        self.assertEqual(len(jobs), 1)
        job = jobs[0]
        self.assertEqual(job["id"], "abc12345")
        self.assertEqual(job["state"], "done")
        self.assertEqual(job["output"], {"result": "final summary"})
        self.assertEqual(job["children"][0]["href"], "https://example.test/pr/1")
        self.assertEqual(job["last_timeline"], {"at": "t2", "state": "done", "detail": "b"})
        self.assertTrue(job["transcript_path"].endswith("t.jsonl"))
        self.assertIsNotNone(job["transcript_mtime"])
        self.assertIsNone(job["live"])
        for hidden in ("respawnFlags", "providerEnv", "linkScanPath"):
            self.assertNotIn(hidden, job)

    def test_joins_live_session_by_job_id(self):
        from claude_chatter.jobs import list_jobs
        live = {"s": {"pid": 5, "kind": "bg", "status": "idle", "parkedJobId": "abc12345", "token": "t"}}
        job = list_jobs(claude_dir=self.tmp.name, live_sessions=live)[0]
        self.assertEqual(job["live"], {"pid": 5, "kind": "bg", "status": "idle"})

    def test_running_requires_live_worker_with_matching_start_time(self):
        from claude_chatter.jobs import list_jobs
        with open("/proc/%d/stat" % os.getpid()) as f:
            start = f.read().rsplit(")", 1)[1].split()[19]
        os.makedirs(os.path.join(self.tmp.name, "daemon"))
        roster = os.path.join(self.tmp.name, "daemon", "roster.json")
        with open(roster, "w") as f:
            json.dump({"workers": {"abc12345": {"pid": os.getpid(), "procStart": start}}}, f)
        self.assertTrue(list_jobs(claude_dir=self.tmp.name)[0]["running"])
        with open(roster, "w") as f:
            json.dump({"workers": {"abc12345": {"pid": os.getpid(), "procStart": "1"}}}, f)
        self.assertFalse(list_jobs(claude_dir=self.tmp.name)[0]["running"])
        os.remove(roster)
        self.assertFalse(list_jobs(claude_dir=self.tmp.name)[0]["running"])

    def test_pr_state_comes_from_cache(self):
        from claude_chatter.jobs import list_jobs
        with open(os.path.join(self.tmp.name, "gh-pr-status-cache.json"), "w") as f:
            json.dump({"https://example.test/pr/1": {"state": "MERGED", "title": "t"}}, f)
        self.assertEqual(list_jobs(claude_dir=self.tmp.name)[0]["children"][0]["pr_state"], "MERGED")

    def test_redact_peers_drops_token_only(self):
        peers = {"a": {"name": "a", "token": "secret", "pid": 1}}
        self.assertEqual(ClaudeMessagingProtocol.redact_peers(peers), {"a": {"name": "a", "pid": 1}})
        self.assertIn("token", peers["a"])


class TestClientCoreOperations(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp_dir.name, "test_core.db")
        self.sock_path = os.path.join(self.tmp_dir.name, "test_core.sock")
        self.session_name = "test-core-sess"

    async def asyncTearDown(self):
        self.tmp_dir.cleanup()

    async def test_client_get_history_pagination_and_ordering(self):
        client = ClaudeClient(
            bridge_socket_path=self.sock_path,
            session_name=self.session_name,
            db_path=self.db_path
        )
        for i in range(1, 6):
            client.response_store[f"msg_{i}"] = {
                "direction": "inbound",
                "content": f"msg {i}",
                "timestamp": float(i)
            }
        single = client.get_history(msg_id="msg_3")
        self.assertIn("history", single)
        self.assertEqual(single["history"]["msg_3"]["content"], "msg 3")

        all_res = client.get_history()
        self.assertEqual(len(all_res["history"]), 5)
        self.assertEqual(list(all_res["history"].keys()), ["msg_5", "msg_4", "msg_3", "msg_2", "msg_1"])

        p1 = client.get_history(limit=2)
        self.assertEqual(list(p1["history"].keys()), ["msg_5", "msg_4"])

        p2 = client.get_history(limit=2, offset=2)
        self.assertEqual(list(p2["history"].keys()), ["msg_3", "msg_2"])
        client.cleanup_socket()

    async def test_client_persistence_error_on_unwritable_db(self):
        from claude_chatter.storage import MessageStore
        bad_dir = os.path.join(self.tmp_dir.name, "bad_dir")
        os.makedirs(bad_dir, exist_ok=True)
        # Block directory creation deterministically for both root and non-root users
        blocking_file = os.path.join(bad_dir, "blocking_file")
        with open(blocking_file, "w", encoding="utf-8") as f:
            f.write("block")
        bad_db = os.path.join(blocking_file, "messages.db")
        store = MessageStore(bad_db)
        self.assertTrue(store.is_disabled())
        self.assertIsNotNone(store.last_error)

        client = ClaudeClient(
            bridge_socket_path=self.sock_path,
            session_name="disabled-store-sess",
            db_path=bad_db
        )
        self.assertIsNotNone(client.persistence_error)
        history = client.get_history()
        self.assertIn("persistence_error", history)
        client.cleanup_socket()

    async def test_client_rename_with_unread_messages_pending(self):
        client = ClaudeClient(
            bridge_socket_path=self.sock_path,
            session_name="pending-sess",
            db_path=self.db_path
        )
        client.response_store["pending_1"] = {
            "direction": "inbound",
            "read": False,
            "content": "unread message payload",
            "timestamp": 1.0
        }
        res = client.rename_session(new_name="renamed-pending-sess")
        self.assertTrue(res["success"])
        self.assertEqual(client.session_name, "renamed-pending-sess")

        unread = client.get_unread_messages(mark_read=False)
        self.assertIn("pending_1", unread)
        self.assertEqual(unread["pending_1"]["content"], "unread message payload")
        client.cleanup_session_descriptor()
        client.cleanup_socket()

    async def test_client_simulated_db_failure_during_rename_raises_storage_error(self):
        import sqlite3
        client = ClaudeClient(
            bridge_socket_path=self.sock_path,
            session_name="original-name",
            db_path=self.db_path
        )
        client.register_session_descriptor(session_name="original-name")

        with mock.patch.object(client.store, "rename_session", side_effect=sqlite3.OperationalError("disk I/O error")):
            with self.assertRaises(StorageError):
                client.rename_session(new_name="failed-name")

        self.assertEqual(client.session_name, "original-name")
        self.assertEqual(client.response_store._session_name, "original-name")
        client.cleanup_session_descriptor()
        client.cleanup_socket()

    async def test_client_get_history_with_persistence_error_preserves_memory(self):
        client = ClaudeClient(
            bridge_socket_path=self.sock_path,
            session_name=self.session_name,
            db_path=self.db_path
        )
        client.response_store["target_msg"] = {
            "direction": "inbound",
            "read": False,
            "content": "test payload",
            "timestamp": time.time()
        }
        client.store._disabled = True
        client.store.last_error = "disk failure simulated"

        res = client.get_history(msg_id="target_msg")
        self.assertIn("persistence_error", res)
        self.assertEqual(res["persistence_error"], "disk failure simulated")
        self.assertIn("target_msg", res["history"])

        self.assertNotIn("persistence_error", client.response_store["target_msg"])
        client.cleanup_socket()

    async def test_client_rename_rejected_when_name_in_use_raises_session_name_in_use_error(self):
        sessions_dir = os.path.join(os.path.expanduser("~"), ".claude", "sessions")
        os.makedirs(sessions_dir, exist_ok=True)

        proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(2.0)"])
        fake_desc = os.path.join(sessions_dir, f"{proc.pid}.json")
        desc_data = {
            "name": "already-active-sess",
            "pid": proc.pid,
            "cwd": "/some/other/cwd"
        }
        with open(fake_desc, "w", encoding="utf-8") as f:
            json.dump(desc_data, f)

        client = ClaudeClient(
            bridge_socket_path=self.sock_path,
            session_name="my-curr-sess",
            db_path=self.db_path
        )
        client.register_session_descriptor(session_name="my-curr-sess")

        try:
            with self.assertRaises(SessionNameInUseError) as ctx:
                client.rename_session(new_name="already-active-sess")
            self.assertIn("currently in use by an active session", str(ctx.exception))
            self.assertEqual(client.session_name, "my-curr-sess")
        finally:
            proc.kill()
            proc.wait()
            if os.path.exists(fake_desc):
                os.remove(fake_desc)
            client.cleanup_session_descriptor()
            client.cleanup_socket()

    async def test_client_message_received_after_rename_stored_under_new_name_and_id(self):
        client = ClaudeClient(
            bridge_socket_path=self.sock_path,
            session_name="orig-sess",
            db_path=self.db_path
        )
        client.register_session_descriptor(session_name="orig-sess")
        client.rename_session(new_name="new-sess")

        server_task = asyncio.create_task(client.start_bridge_listener())
        await asyncio.sleep(0.1)

        reader, writer = await asyncio.open_unix_connection(self.sock_path)
        msg_id = "after_rename_msg_1"
        frame = {
            "msgV": 1,
            "msg_id": msg_id,
            "type": "user",
            "message": {
                "role": "user",
                "content": "message after rename"
            },
            "sender": "sender_peer"
        }
        writer.write((json.dumps(frame) + "\n").encode("utf-8"))
        await writer.drain()
        await asyncio.sleep(0.1)
        writer.close()
        await writer.wait_closed()

        self.assertIn(msg_id, client.response_store)
        self.assertEqual(client.response_store[msg_id]["content"], "message after rename")

        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass

        client.cleanup_session_descriptor()
        client.cleanup_socket()

        from claude_chatter.storage import MessageStore
        check_store = MessageStore(self.db_path)
        conn = check_store._get_connection()
        cur = conn.execute("SELECT session_name, session_id FROM messages WHERE msg_id = ?", (msg_id,))
        row = cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["session_name"], "new-sess")
        self.assertEqual(row["session_id"], client.session_id)
        check_store.close()


if __name__ == "__main__":
    unittest.main()
