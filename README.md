# claude-chatter

Claude Code peer messaging protocol and client library.

`claude-chatter` provides a standalone Python implementation of the Claude Code peer-to-peer messaging protocol. It handles peer discovery via local session descriptors (`~/.claude/sessions/*.json`), background jobs inspection (`~/.claude/jobs`), UNIX domain socket IPC, NDJSON message framing, and local SQLite mailbox persistence.

This library has no dependencies on FastMCP or Antigravity and can be used directly in any Python application or CLI workflow.

## Installation

```bash
pip install claude-chatter
```

## Quick Start

### As a Library

```python
import asyncio
from claude_chatter import ClaudeClient, ClaudeWire

async def main():
    # Stateless discovery
    sessions = ClaudeWire.scan_sessions()
    print("Discovered sessions:", sessions)

    # Initialize client node
    client = ClaudeClient(session_name="my-worker")

    # Send message to an active session
    res = await client.send_message(
        target_session="other-session",
        message_content="Hello from claude-chatter"
    )
    print("Send status:", res)

if __name__ == "__main__":
    asyncio.run(main())
```

### Inbound Message Callback

```python
async def on_message(event):
    print(f"Received frame {event['msg_id']} from {event['sender']}: {event['content']}")

client = ClaudeClient(session_name="listener", on_inbound_message=on_message)
```

An inbound `{"type":"auth","token":"..."}` line (`peerToken` is accepted as the key too) is checked against the token this process registered, and the connection is closed on a mismatch. `ClaudeClient(require_auth=True)` additionally refuses any connection that does not open with a valid auth line.

`event["sender"]` is the live session that owns the connecting process (or an ancestor of it) when one can be found, with `event["sender_verified"] = True`. Otherwise it is the name the frame claims and `sender_verified` is `False`. `claimed_sender`, `peer_pid` and the wrapper's `from_mode` are always included. Peer lookup needs Linux (`SO_PEERCRED`) and a sender that is still running when the connection is accepted.

### CLI Tool

The package installs both `claude-chatter` and `chatter` CLI entry points:

```bash
# List all active Claude Code sessions
claude-chatter --list

# List Claude Code background jobs
claude-chatter --jobs

# Inspect unread messages from local mailbox
claude-chatter --inbox

# View full persisted message history
claude-chatter --history

# Send message to a target session
claude-chatter --send target-session "Hello peer"

# Run standalone daemon socket listener
claude-chatter --standalone --name my-daemon

# Proactively scan and purge dead session files
claude-chatter --purge
```

## Provenance

`claude-chatter` was extracted from [non7top/claude-chatter-mcp](https://github.com/non7top/claude-chatter-mcp) to provide a reusable, host-independent library for the Claude Code peer messaging protocol with zero MCP dependencies. Full commit history of the protocol core was preserved during the separation.

## License

MIT
