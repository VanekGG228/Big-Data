from save_To_Json import SaveToJson
from save_To_XML import SaveToXML
from students_parser import StudentParser
from rooms_parser import RoomParser
from database_manager import DatabaseManager

class Handler:
    def __init__(self):
        self.db = DatabaseManager("Campus", "localhost", 3306, "root", "Val81248124val")

    def add(self,rooms_path, students_path):
        self.db.add_data(rooms_path,students_path)
    def get_students(self):
        students = self.db.get_data_students()
        if len(students)<5:
            return students
        for i in range(5):
            print(students[i])
        return students

    def get_rooms(self):
        rooms = self.db.get_data_rooms()
        if len(rooms)<5:
            return rooms
        for i in range(5):
            print(rooms[i])
        return rooms

    def save_rooms_to_json(self, file_path):
        SaveToJson.save_table_to_json(RoomParser(self.db.get_data_rooms()), file_path)

    def save_rooms_to_xml(self, file_path):
        SaveToXML.save_table_to_xml(RoomParser(self.db.get_data_rooms()), file_path)

    def save_students_to_json(self, file_path):
        SaveToJson.save_table_to_json(StudentParser(self.db.get_data_students()), file_path)

    def save_students_to_xml(self, file_path):
        SaveToXML.save_table_to_xml(StudentParser(self.db.get_data_students()), file_path)

handler = Handler()


while (1):
    string = input()
    if string == "add":
        handler.add("rooms.json","students.json")
    elif string == "students":
        handler.get_students()
    elif string == "rooms":
        handler.get_rooms() 
    elif string == "format":
        string = input("Type ")
        file_path = input("File path ")
        table = input("Table ")
        if string == "json":
            if table == "rooms":
                handler.save_rooms_to_json(file_path)
            else:
                handler.save_students_to_json(file_path)    
        else:
            if table == "rooms":
                handler.save_rooms_to_xml(file_path)
            else:
                handler.save_students_to_xml(file_path) 
    else:
        break            