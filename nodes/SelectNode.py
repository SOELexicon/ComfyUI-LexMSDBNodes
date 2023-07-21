import  io, torch
import numpy as np
from PIL import Image

class SelectNode:
    def __init__(self, table_name = 'txt2img'):
        self.table_name = table_name
        self.RETURN_TYPES = self.getFields()
        self.RETURN_NAMES = self.getFieldsNames()
    table_name ="None"
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "id": ("INT", {"default": "1"})
            },
        }

    @property
    def RETURN_TYPES(self):
        return self.getFields()

    @property
    def RETURN_NAMES(self):
        return self.getFieldsNames()

    FUNCTION = "execute_query"
    CATEGORY = "LexNodes.MSSQL"

    def getFields(self):
        global table_info
        fields = table_info[self.table_name]
        return tuple(fields.keys())
    
    def getFieldsNames(self):
        global table_info
        fields = table_info[self.table_name]
        return tuple(fields.values())
    
    def execute_query(self, **kwargs):
        global conn,table_info
        cursor = conn.cursor()

        columns = ', '.join(table_info[self.table_name].keys())
        sql = f"SELECT {columns} FROM {self.table_name} WHERE Id = {kwargs.get('id')}"
        cursor.execute(sql)
        results = cursor.fetchall()
        all_rows = []
        
        for result in results:
            result_dict = {}
            for column, value in zip(table_info[self.table_name].keys(), result):
                if column =='Image':  # Check if the value is a bytearray
                    image = Image.open(io.BytesIO(value))
                    # Perform additional image processing
                    image = image.convert("RGB")
                    image = np.array(image).astype(np.float32) / 255.0
                    image = torch.from_numpy(image)[None,]
                    result_dict[column] = image
                else:
                    result_dict[column] = value
            all_rows.append(tuple(result_dict.values()))

        return all_rows[0]
NODE_CLASS_MAPPINGS = {
    "SelectNode": SelectNode
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "SelectNode": "Sql Select Node"
}