from abc import ABC, abstractmethod

class Table:
    @abstractmethod
    def loadJson(self,file_path):
        pass
    @abstractmethod
    def getStrings(self):
        pass
    @abstractmethod
    def sql_insert(self):
        pass