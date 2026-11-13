"""Exceptions for the claude_chatter library."""


class ChatterError(Exception):
    """Base exception for all claude_chatter errors."""
    pass


class SessionNameInUseError(ChatterError):
    """Raised when a target session name is already in use by an active session."""
    pass


class AmbiguousSessionError(ChatterError):
    """Raised when a target name matches more than one live session."""

    def __init__(self, target, candidates):
        self.target = target
        self.candidates = candidates
        super().__init__(f"Target '{target}' matches {len(candidates)} live sessions")


class StorageError(ChatterError):
    """Raised when an operation on message or session storage fails."""
    pass
