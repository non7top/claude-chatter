"""Exceptions for the claude_chatter library."""


class ChatterError(Exception):
    """Base exception for all claude_chatter errors."""
    pass


class SessionNameInUseError(ChatterError):
    """Raised when a target session name is already in use by an active session."""
    pass


class StorageError(ChatterError):
    """Raised when an operation on message or session storage fails."""
    pass
