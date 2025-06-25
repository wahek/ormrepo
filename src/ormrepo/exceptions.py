from typing import Any


class BaseExceptionORM(Exception):
    def __init__(self, message: str,
                 status_code: int = 400,
                 detail: dict[str: Any] = None):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.detail = detail

    def __str__(self):
        return ((f'{self.message}, '
                 f'code: {self.status_code}, '
                 f'detail: {self.detail}')
                if self.detail
                else (f'{self.message}, '
                      f'code: {self.status_code}'))


class ConfigurateException(BaseExceptionORM):
    def __init__(self, message: str = 'Configurate Error',
                 status_code: int = 500,
                 detail: dict[str: Any] = None):
        super().__init__(message, status_code, detail)

class ORMException(BaseExceptionORM):
    def __init__(self, message: str = 'ORM Error',
                 status_code: int = 400,
                 detail: dict[str: Any] = None):
        super().__init__(message, status_code, detail)

class EntryNotFound(ORMException):
    def __init__(self, message: str = 'Entry not found',
                 status_code: int = 404,
                 detail: dict[str: Any] = None):
        super().__init__(message, status_code, detail)
