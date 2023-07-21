import datetime
import pyodbc
import io, torch
import numpy as np
from PIL import Image

class TableNode:
    table_name="txt2img"
    fields = {}
    def __init__(self, db_connection, table_info):
        self.conn = db_connection.connect()
        self.table_info = table_info
        self.table_name = "txt2img"
        print(self.table_info)


    @classmethod
    def INPUT_TYPES(cls):
        print()
        tables = list(cls.table_info.keys())
        fields = cls.table_info[cls.table_name]

        # Create an input for each field
        inputs = {"Table": (tables,)}
        for field, field_type in fields.items():
            if field_type == int:
                input_type = "INT"
                default_value = 0
            elif field_type == str:
                input_type = "STRING"
                default_value = ""
            elif field_type == bool:
                input_type = "STRING"
                default_value = "False"
            elif field_type == float:
                input_type = "FLOAT"
                default_value = 0.0
            elif field_type == bytearray:
                input_type = "IMAGE"
                default_value = None
            else:
                input_type = "STRING"  # default type
                default_value = ""

            inputs[field] = (input_type, {"default": default_value})

        print({"required": inputs})
        return {"required": inputs}

    RETURN_TYPES = ("STRING", "INT",)
    FUNCTION = "execute_query"
    CATEGORY = "LexNode.MSSQL"

    def execute_query(self, **kwargs):
        cursor = self.conn.cursor()
        
        # Set the table name to the value of the "Table" field and remove it from kwargs
        self.table_name = kwargs.pop('Table', self.table_name)

        if kwargs.get('id', None) == 0:  # Check if id is 0
            # Remove 'id' from kwargs
            kwargs.pop('id', None)
            
            # Prepare the SQL statement for inserting a new record
            kwargs['DateAdded'] = datetime.datetime.now()  # Add current datetime
            columns = ', '.join(kwargs.keys())
            placeholders = ', '.join('?' for _ in kwargs)
            sql = f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})"
            
            values = []
            for value in kwargs.values():
                if isinstance(value, torch.Tensor):  # Check if the value is a PyTorch tensor
                    # Convert the tensor to a numpy array
                    i = 255. * value[0].cpu().numpy()
                    img = Image.fromarray(np.clip(i, 0, 255).astype(np.uint8))
                    byte_array = io.BytesIO()
                    img.save(byte_array, format='JPEG')
                    value = pyodbc.Binary(byte_array.getvalue())
                elif isinstance(value, datetime.datetime):  # Check if the value is a datetime object
                    value = value.strftime('%Y-%m-%d %H:%M:%S')  # Format the datetime object to string
                values.append(value)

            cursor.execute(sql, tuple(values))
            self.conn.commit()  # Don't forget to commit the changes
            cursor.execute("SELECT @@IDENTITY AS 'Identity'")
            id_of_new_row = cursor.fetchone()[0]
            return ["Insert operation completed.", id_of_new_row]
        else:
            for field, value in kwargs.items():
                if field in self.table_info[self.table_name]:  # Ensure the field exists in the table
                    if isinstance(value, torch.Tensor):  # Check if the value is a numpy array
                        # Ensure the numpy array can be represented as an image
                        i = 255. * value[0].cpu().numpy()
                        img = Image.fromarray(np.clip(i, 0, 255).astype(np.uint8))
                        # Convert the numpy array to a byte array
                        byte_arr = io.BytesIO()
                        img.save(byte_arr, format='JPEG')
                        value = pyodbc.Binary(byte_arr.getvalue())
                    cursor.execute(f"UPDATE {self.table_name} SET {field} = ? WHERE {field} = ?", (value, value))
            self.conn.commit()  # Don't forget to commit the changes
            return ["Update operation completed.", kwargs.get('id')]

NODE_CLASS_MAPPINGS = {
    "TableNode": TableNode
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "TableNode": "Sql Table Node"
    }