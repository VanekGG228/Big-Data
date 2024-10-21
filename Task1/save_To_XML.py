
class SaveToXML:
    @staticmethod
    def save_table_to_xml(parser, file_path,query_name, tree_element):
        tree = parser.to_XML(query_name, tree_element)
        with open(file_path, "wb") as f:
                tree.write(f, encoding="utf-8", xml_declaration=True)