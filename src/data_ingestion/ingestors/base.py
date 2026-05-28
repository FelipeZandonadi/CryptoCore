from abc import ABC, abstractmethod


class BaseIngestor(ABC):
    @abstractmethod
    def run(self) -> None: ...
