from table import Table
import json

class Student(Table):
    def __init__(self, db):
        self.students = None
        self.db_name = db

    def loadJson(self,file_path):
        with open(file_path, 'r') as file:
            self.students = json.load(file)

    def getStrings(self):
        tuple_students = []
        for student in self.students:
            tuple_students.append((student["birthday"],student["id"],student["name"],student["room"],student["sex"]))
        return tuple_students
    def sql_insert(self):
        return f"INSERT INTO {self.db_name}.students (birthday, id, name, room, sex) VALUES (%s, %s,%s, %s, %s)"