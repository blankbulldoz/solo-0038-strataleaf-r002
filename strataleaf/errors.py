class DatabaseError(Exception):
    """Base error for caller-visible database failures."""

class InvalidDocument(DatabaseError):
    pass

class Conflict(DatabaseError):
    pass

class NotFound(DatabaseError):
    pass

class SnapshotTooOld(DatabaseError):
    pass

class InvalidCursor(DatabaseError):
    pass

class InvalidReference(DatabaseError):
    """A fixed-version reference is malformed, dangling, or no longer resolvable."""
    pass

class ReadOnly(DatabaseError):
    pass

class ReplicationError(DatabaseError):
    pass
