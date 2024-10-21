from parser import Parser
import xml.etree.ElementTree as ET

class RoomParser(Parser):
    def __init__(self,rooms):
        self.rooms = rooms
    def to_Json(self):
        formatted = []
        for room in self.rooms:
            formatted.append({"id":room[0],"name":room[1]})
        return formatted
    def to_XML(self):
        root = ET.Element("rooms")
        for room in self.rooms:
            room_element = ET.SubElement(root, "room")
            room_id_element = ET.SubElement(room_element, "id")
            room_id_element.text = str(room[0])
            name_element = ET.SubElement(room_element, "name")
            name_element.text = room[1]

        return ET.ElementTree(root)