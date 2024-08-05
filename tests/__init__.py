from typing import (Generator,
                    Optional,
                    TypeVar)

T = TypeVar('T')
YieldFixture = Generator[T, None, None]


class ColumnarTestEnvironmentException(Exception):
    """Raised when something with the test environment is incorrect."""

    def __init__(self, message: Optional[str] = None) -> None:
        super().__init__(message)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({super().__repr__()})"

    def __str__(self) -> str:
        return self.__repr__()
