class SocrataError(Exception):
    pass


class SocrataTypeError(SocrataError, TypeError):
    def __init__(self, field: str, expected: str, actual):
        msg = f"{field} should be {expected} type -- got {actual} instead"
        super().__init__(msg)

        self.field = field
        self.expected = expected
        self.actual = actual


class SocrataColumnNameTooLong(SocrataError, ValueError):
    def __init__(self, field: str):
        super().__init__(f'The field "{field}" is too long')
