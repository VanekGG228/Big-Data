from parser import Parser
import xml.etree.ElementTree as ET
from datetime import date

class StudentParser(Parser):
    def __init__(self, students):
        self.students = students

    def to_Json(self):
        formatted = []
        for student in self.students:
            formatted.append({"id":student[0],"name":student[1],"birthday":student[2].isoformat(),"room":student[3],"sex":student[4]})
        return formatted

    def to_XML(self):
        root = ET.Element("students")
        for student in self.students:
            student_element = ET.SubElement(root, "student")

            stud_id = ET.SubElement(student_element, "id")
            stud_id.text = str(student[0])

            stud_name = ET.SubElement(student_element, "name")
            stud_name.text = student[1]

            stud_birth = ET.SubElement(student_element, "birthday")
            stud_birth.text = str(student[2])

            stud_room = ET.SubElement(student_element, "room")
            stud_room.text = str(student[3])

            stud_sex = ET.SubElement(student_element, "sex")
            stud_sex.text = student[4]

        return ET.ElementTree(root)