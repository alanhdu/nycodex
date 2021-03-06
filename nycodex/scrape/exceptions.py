from typing import Any


class SocrataError(Exception):
    pass


class SocrataTypeError(SocrataError, TypeError):
    def __init__(self, field: str, expected: str, actual: Any) -> None:
        msg = f"{field} should be {expected} type -- got {actual} instead"
        super().__init__(msg)

        self.field = field
        self.expected = expected
        self.actual = actual


class SocrataColumnNameTooLong(SocrataError, ValueError):
    def __init__(self, field: str) -> None:
        super().__init__(f'The field "{field}" is too long')


class SocrataParseError(SocrataError, ValueError):
    pass


class SocrataDatasetTooLarge(SocrataError, RuntimeError):
    pass
