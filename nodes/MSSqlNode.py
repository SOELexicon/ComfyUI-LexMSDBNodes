import datetime
import pyodbc
import configparser, os, io, torch
import numpy as np
import cv2
from PIL import Image

# Global variables
table_info = {}
conn = None
connection_string = ''
fields ={}

class MSSQLFn:
    @staticmethod
    def load_table_info():
        global table_info, conn
        cursor = conn.cursor()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'")
        tables = [row.table_name for row in cursor.fetchall()]
        for table in tables:
            cursor.execute(f"SELECT * FROM {table}")
            columns = {column[0]: column[1] for column in cursor.description}
           # print(columns)

            table_info[table] = columns
        fields = table_info['txt2img']

    @staticmethod
    def getFields(self):
        MSSQLFn.load_table_info()
        fields = table_info['txt2img']
        field_types = {}
        for field, field_type in fields.items():
            if field_type == int:
                input_type = "INT"
            elif field_type == str:
                input_type = "STRING"
            elif field_type == bool:
                input_type = "STRING"
            elif field_type == float:
                input_type = "FLOAT"
            elif field_type == bytearray:
                input_type = "IMAGE"
            else:
                input_type = "STRING"  # default type
            field_types[field] = input_type
      #  print(field_types)
        return tuple(field_types.values())
    
    @staticmethod
    def getFieldsNames(self):
        MSSQLFn.load_table_info()
        fields = table_info['txt2img']
        field_types = {}
        for field, field_type in fields.items():
            if field_type == int:
                input_type = "INT"
            elif field_type == str:
                input_type = "STRING"
            elif field_type == bool:
                input_type = "STRING"
            elif field_type == float:
                input_type = "FLOAT"
            elif field_type == bytearray:
                input_type = "IMAGE"
            else:
                input_type = "STRING"  # default type
            field_types[field] = input_type
      #  print(field_types)
        return tuple(field_types.keys())
    @staticmethod
    def readConfig():
        global connection_string
        config = configparser.ConfigParser()
        current_dir = os.path.dirname(os.path.realpath(__file__))
        config_path = os.path.join(current_dir, 'config.ini')
        config.read(config_path)
        server = config['MSSQL']['server']
        database = config['MSSQL']['database']
        username = config['MSSQL']['username']
        password = config['MSSQL']['password']
        driver = config['MSSQL']['driver']
        integrated_security = config.getboolean('MSSQL', 'integrated_security', fallback=False)

        connection_string = f'DRIVER={{{driver}}};SERVER={server};DATABASE={database};'
        if integrated_security:
            connection_string += 'Trusted_Connection=yes;'
        else:
            connection_string += f'UID={username};PWD={password};'
        return connection_string

    @staticmethod
    def connect():
        global conn, connection_string
        if connection_string == '':
            MSSQLFn.readConfig()
        if conn is None:
            conn = pyodbc.connect(connection_string)
        return conn

class MSSQLQueryNode:
    @classmethod
    def INPUT_TYPES(s):
        return {
            "required": {
                "query": ("STRING", {"multiline": True, "default": "SELECT top 1 * FROM Prompts"}),
            },
        }

    RETURN_TYPES = ("STRING",)
    FUNCTION = "execute_query"
    CATEGORY = "LexNode.MSSQL"

    def execute_query(self, query):
        self.conn =MSSQLFn.connect(self)
        cursor = self.conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        return (str(result),)
    
conn = None
connection_string = ''
self ={"conn":conn,"connection_string":connection_string}   
MSSQLFn.readConfig()
MSSQLFn.connect()
MSSQLFn.load_table_info()
fields =MSSQLFn.getFields(self)


class MSSqlTableNode:
   
    @classmethod
    def INPUT_TYPES(cls):
        global table_info

        print( cls)
        tables = list(table_info.keys())
        cls.table_name = "txt2img"
        fields = table_info[cls.table_name]
        # Create an input for each field
        inputs = {}
        inputs["Table"] = (tables,)
        
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
    
    RETURN_TYPES = ("STRING","INT",)
    FUNCTION = "execute_query"
    CATEGORY = "LexNode.MSSQL"

    
    def execute_query(self, **kwargs):
        global conn
        cursor = conn.cursor()
        
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
        #   print (sql)
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

    #     print (tuple(values))
            cursor.execute(sql, tuple(values))

            conn.commit()  # Don't forget to commit the changes
            cursor.execute("SELECT @@IDENTITY AS 'Identity'")
            id_of_new_row = cursor.fetchone()[0]
            return ["Insert operation completed.", id_of_new_row]
        else:
            for field, value in kwargs.items():
                if field in table_info[self.table_name]:  # Ensure the field exists in the table
                    if isinstance(value, torch.Tensor):  # Check if the value is a numpy array
                        # Ensure the numpy array can be represented as an image
                            i = 255. * value[0].cpu().numpy()
                            img = Image.fromarray(np.clip(i, 0, 255).astype(np.uint8))
                            # Convert the numpy array to a byte array
                            byte_arr = io.BytesIO()
                            img.save(byte_arr, format='JPEG')
                            value = pyodbc.Binary(byte_arr.getvalue())
                    cursor.execute(f"UPDATE {self.table_name} SET {field} = ? WHERE {field} = ?", (value, value))
            conn.commit()  # Don't forget to commit the changes
            return ["Update operation completed.", kwargs.get('id')]


class MSSqlSelectNode:
    def __init__(self):
        self.table_name = 'txt2img'
        self.RETURN_TYPES = MSSQLFn.getFields(self)
        self.RETURN_NAMES = MSSQLFn.getFieldsNames(self)

    @classmethod
    def INPUT_TYPES(cls):
        global table_info,fields
        
        cls.table_name = 'txt2img'
        fields = table_info[cls.table_name]
    
       
        return {
            "required": {
                "id": ("INT", {"default": "1"})
            },
        }

    RETURN_TYPES = MSSQLFn.getFields(self)
    RETURN_NAMES = MSSQLFn.getFieldsNames(self)
    FUNCTION = "execute_query"
    CATEGORY = "LexNode.MSSQL"

    def getFields(self):
        global table_info
        fields = table_info[self.table_name]
        return tuple(fields.keys())
    
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



class MSSqlNode:
    @classmethod
    def INPUT_TYPES(s):
        global table_info
        return {
            "required": {
                "table": ("STRING", {"default": list(table_info.keys())[0]}),
                "field": ("STRING", {"default": "*"}),
            },
        }

    RETURN_TYPES = ("STRING",)
    FUNCTION = "execute_query"
    CATEGORY = "LexNode.MSSQL"

    def execute_query(self, table, field):
        global conn
        cursor = conn.cursor()
        cursor.execute(f"SELECT {field} FROM {table}")
        result = cursor.fetchall()
        return result

NODE_CLASS_MAPPINGS = {
    "MSSQLQuery": MSSQLQueryNode,
    "MSSqlNode": MSSqlNode,
    "MSSqlTableNode": MSSqlTableNode,
    "MSSqlSelectNode": MSSqlSelectNode,
}
