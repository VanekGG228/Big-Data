
class SaveToXML:
    @staticmethod
    def save_table_to_xml(parser, file_path):
        tree = parser.to_XML()
        with open(file_path, "wb") as f:
                tree.write(f, encoding="utf-8", xml_declaration=True)