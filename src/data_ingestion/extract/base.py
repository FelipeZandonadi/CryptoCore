from abc import ABC, abstractmethod


class BaseExtractor(ABC):
    @abstractmethod
    def batch(self, target: str, fullname: str, limit: int = 25) -> list[dict]: ...
