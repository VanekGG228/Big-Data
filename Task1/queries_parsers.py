from parser import Parser
import xml.etree.ElementTree as ET
from decimal import Decimal

class QueriesParser(Parser):
    def __init__(self, data, columns):
        self.data = data
        self.columns = columns



    def to_Json(self):
        formatted = []
        for data in self.data:
            row_dict = dict(zip(self.columns, data))
            for key, value in row_dict.items():
                if isinstance(value, Decimal):
                    row_dict[key] = float(value) 
            
            formatted.append(row_dict)
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
