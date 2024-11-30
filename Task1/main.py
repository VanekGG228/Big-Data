from save_To_Json import SaveToJson
from save_To_XML import SaveToXML
from queries_parsers import QueriesParser
from database_manager import DatabaseManager
from dotenv import load_dotenv 
import os

class Handler:
    def __init__(self):
        load_dotenv()        
        db_name = os.getenv("DB_NAME")
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        
        self.db = DatabaseManager(db_name, db_host, int(db_port), db_user, db_password)

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

    def students_count_json(self, file_path):
        data = self.db.get_room_student_counts()
        SaveToJson.save_table_to_json(QueriesParser(data,["room","amount"]),file_path)

    def students_count_xml(self, file_path):
        data = self.db.get_room_student_counts()
        SaveToXML.save_table_to_xml(QueriesParser(data,["room","amount"]),file_path,"students_count","ROOM")


    def room_age_json(self, file_path):
        data = self.db.get_top_5_youngest_rooms()
        SaveToJson.save_table_to_json(QueriesParser(data,["room","age"]),file_path)

    def room_age_xml(self, file_path):
        data = self.db.get_top_5_youngest_rooms()
        SaveToXML.save_table_to_xml(QueriesParser(data,["room","age"]),file_path,"rooms_average_age","ROOM")


    def room_diff_json(self, file_path):
        data = self.db.get_top_5_age_difference_rooms()
        SaveToJson.save_table_to_json(QueriesParser(data,["room","diff"]),file_path)

    def room_diff_xml(self, file_path):
        data = self.db.get_top_5_age_difference_rooms()
        SaveToXML.save_table_to_xml(QueriesParser(data,["room","diff"]),file_path,"rooms_difference_age","ROOM")


    def room_FM_json(self, file_path):
        data = self.db.get_mixed_gender_rooms()
        SaveToJson.save_table_to_json(QueriesParser(data,["room"]),file_path)

    def room_FM_xml(self, file_path):
        data = self.db.get_mixed_gender_rooms()
        SaveToXML.save_table_to_xml(QueriesParser(data,["room"]),file_path,"rooms_FM","ROOM")    
        
handler = Handler()
queries = [
    "Список комнат и количество студентов в каждой из них  -  1",
    "5 комнат, где самый маленький средний возраст студентов  -  2",
    "5 комнат с самой большой разницей в возрасте студентов  -  3",
    "Список комнат, где живут разнополые студенты  -  4"
]

for query in queries:
    print(query)

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
        query = input("Query ")
        if string == "json":
            if query == "1":
                handler.students_count_json(file_path)
            elif query == "2":
                handler.room_age_json(file_path)
            elif query == "3":
                handler.room_diff_json(file_path)  
            else:
                handler.room_FM_json(file_path) 
        else:
            if query == "1":
                handler.students_count_xml(file_path)
            elif query == "2":
                handler.room_age_xml(file_path)
            elif query == "3":
                handler.room_diff_xml(file_path)  
            else:
                handler.room_FM_xml(file_path) 
                
    else:
        break            