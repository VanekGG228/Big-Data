
import mysql.connector
from students_table import Student
from rooms_table import Room

class DatabaseManager:
    def __init__(self, name, _host, _port, _user, _password):
        self.connection = mysql.connector.connect(
            host=_host,
            port=_port,
            user=_user,
            password=_password)
        self.cursor = self.connection.cursor()
        self.db_name = name
        self.tables =[Room(name),Student(name)]

    def add_data(self,path_rooms,path_students):
        self.__insert_table(self.tables[0],path_rooms)
        self.__insert_table(self.tables[1],path_students)
        self.connection.commit()

    def get_room_student_counts(self):
        query = f"""
        SELECT room, COUNT(id) 
        FROM {self.db_name}.students
        GROUP BY room;
        """
        self.cursor.execute(query)      
        return self.cursor.fetchall()

    def get_top_5_youngest_rooms(self):
        query = f"""
        SELECT room, AVG(DATEDIFF(CURDATE(), birthday) / 365) as age 
        FROM {self.db_name}.students
        GROUP BY room
        ORDER BY age
        LIMIT 5;
        """
        self.cursor.execute(query)      
        return self.cursor.fetchall()

    def get_top_5_age_difference_rooms(self):
        query = f"""
        SELECT room, MAX(DATEDIFF(CURDATE(), birthday)/365) - MIN(DATEDIFF(CURDATE(), birthday)/365) AS diff
        FROM {self.db_name}.students 
        GROUP BY room
        ORDER BY diff DESC
        LIMIT 5;
        """
        self.cursor.execute(query)      
        return self.cursor.fetchall()

    def get_mixed_gender_rooms(self):
        query = f"""
        SELECT room 
        FROM {self.db_name}.students
        GROUP BY room
        HAVING COUNT(DISTINCT sex) = 2;
        """
        self.cursor.execute(query)      
        return self.cursor.fetchall()
        
    
    def get_data_rooms(self):
        self.cursor.execute(f"SELECT * FROM {self.db_name}.rooms ")  
        return self.cursor.fetchall()

    def get_data_students(self):
        self.cursor.execute(f"SELECT * FROM {self.db_name}.students ")  
        return self.cursor.fetchall()

    def __insert_table(self,table,file_path):
        table.loadJson(file_path)
        self.cursor.executemany(table.sql_insert(),table.getStrings())  