from __future__ import annotations


class APIException(Exception):
    def __init__(self, *, status_code: int, error_code: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code
        self.message = message


class UnauthorizedError(APIException):
    def __init__(self, message: str = "Требуется авторизация") -> None:
        super().__init__(status_code=401, error_code="unauthorized", message=message)


class ForbiddenError(APIException):
    def __init__(self, message: str = "Недостаточно прав доступа") -> None:
        super().__init__(status_code=403, error_code="forbidden", message=message)


class NotFoundError(APIException):
    def __init__(self, *, error_code: str, message: str) -> None:
        super().__init__(status_code=404, error_code=error_code, message=message)


class SemanticValidationError(APIException):
    def __init__(self, *, error_code: str, message: str) -> None:
        super().__init__(status_code=422, error_code=error_code, message=message)


class ServiceUnavailableError(APIException):
    def __init__(self, *, error_code: str, message: str) -> None:
        super().__init__(status_code=503, error_code=error_code, message=message)


class StaleDataError(APIException):
    def __init__(self, message: str = "Данные рейтинга устарели") -> None:
        super().__init__(status_code=503, error_code="stale_ranking", message=message)
