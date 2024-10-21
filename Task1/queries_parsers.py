from parser import Parser
import xml.etree.ElementTree as ET

class QueriesParser(Parser):
    def __init__(self, data, columns):
        self.data = data
        self.columns = columns

    def to_Json(self):
        formatted = []
        for data in self.data:
            formatted.append(dict(zip(self.columns, data)))
        return formatted 

    def to_XML(self, query_name, tree_element ):
        root = ET.Element(query_name)
        for data in self.data:
            element = ET.SubElement(root, tree_element)
            for i, column in enumerate(self.columns):
                query_element = ET.SubElement(element, column)
                if not isinstance(data[i], str):
                    query_element.text = str(data[i])

        return ET.ElementTree(root)


class RoomAgeParser(Parser):
    def __init__(self,data):
        self.data = data
    def to_Json(self):
        formatted = []
        for data in self.data:
            formatted.append({"id":data[0],"age":data[1]})
        return formatted 


    def to_XML(self):
        pass

class RoomDiffParser(Parser):
    def __init__(self,data):
        self.data = data
    def to_Json(self):
        formatted = []
        for data in self.data:
            formatted.append({"id":data[0],"diff":data[1]})
        return formatted 


    def to_XML(self):
        pass

class RoomMFParser(Parser):
    def __init__(self,data):
        self.data = data
    def to_Json(self):
        formatted = []
        for data in self.data:
            formatted.append({"id":data[0]})
        return formatted 


    def to_XML(self):
        pass            