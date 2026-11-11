"""
Backward-compatibility alias module.
Exports ClaudeMessagingProtocol = ClaudeClient.
"""
from .client import ClaudeClient

ClaudeMessagingProtocol = ClaudeClient

__all__ = ["ClaudeClient", "ClaudeMessagingProtocol"]
