from table import Table
import json


class Room(Table):
    def __init__(self, db):
        self.rooms = None
        self.db_name = db

    def loadJson(self,file_path):
        with open(file_path, 'r') as file:
            self.rooms = json.load(file)

    def getStrings(self):
        tuple_rooms = []
        for room in self.rooms:
            tuple_rooms.append((room["id"],room["name"]))
        return tuple_rooms

    def sql_insert(self):
        return f"INSERT INTO {self.db_name}.rooms (id, name) VALUES (%s, %s)"