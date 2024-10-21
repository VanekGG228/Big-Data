import json

class SaveToJson:
    @staticmethod
    def save_table_to_json(parser, file_path):
        json_data = parser.to_Json()
        with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=4)