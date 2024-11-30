from abc import ABC, abstractmethod
class Parser:
    @abstractmethod
    def to_Json(self):
        pass
    @abstractmethod
    def to_XML(self):
        pass