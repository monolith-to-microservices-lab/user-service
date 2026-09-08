class UserNotFoundError(Exception):
    def __init__(self, user_id: int) -> None:
        self.user_id = user_id
        super().__init__(f"user {user_id} not found")


class ImportConflictError(Exception):
    """Raised when an import targets an existing id but carries different data."""

    def __init__(self, user_id: int, conflicts: dict[str, dict[str, str]]) -> None:
        self.user_id = user_id
        self.conflicts = conflicts
        super().__init__(f"import conflict for user {user_id}: {conflicts}")
