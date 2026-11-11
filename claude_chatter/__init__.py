"""
claude-chatter: Generic peer messaging and session protocol library for Claude Code.
"""
from .wire import ClaudeWire
from .client import ClaudeClient
from .protocol import ClaudeMessagingProtocol
from .jobs import list_jobs
from .storage import MessageStore, ResponseStore
from .exceptions import ChatterError, SessionNameInUseError, StorageError

__all__ = [
    "ClaudeWire",
    "ClaudeClient",
    "ClaudeMessagingProtocol",
    "list_jobs",
    "MessageStore",
    "ResponseStore",
    "ChatterError",
    "SessionNameInUseError",
    "StorageError",
]
