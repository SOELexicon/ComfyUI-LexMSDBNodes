import configparser
import os
import pyodbc
from .models.TableInformation import TableInformation
from .nodes.QueryNode import QueryNode
from .nodes.TableNode import TableNode
from .nodes.SelectNode import SelectNode


class DatabaseConnection:
    def __init__(self, config_file='config.ini'):
        self.config_file = config_file
        self.conn = None
        self.connection_string= None
        self._load_config()

    def _load_config(self):
        config = configparser.ConfigParser()
        current_dir = os.path.dirname(os.path.realpath(__file__))
        config_path = os.path.join(current_dir, self.config_file)
        config.read(config_path)
        server = config['MSSQL']['server']
        database = config['MSSQL']['database']
        username = config['MSSQL']['username']
        password = config['MSSQL']['password']
        driver = config['MSSQL']['driver']
        integrated_security = config.getboolean('MSSQL', 'integrated_security', fallback=False)

        self.connection_string = f'DRIVER={{{driver}}};SERVER={server};DATABASE={database};'
        if integrated_security:
            self.connection_string += 'Trusted_Connection=yes;'
        else:
            self.connection_string += f'UID={username};PWD={password};'
        return self.connection_string
    def connect(self):
        if self.conn is None:
            self.conn = pyodbc.connect(self.connection_string)
        return self.conn

# Create a connection and table info instance to pass to the node classes
db_connection = DatabaseConnection('config.ini')
table_info = TableInformation(db_connection.connect())

# Node classes
node_classes = [QueryNode, TableNode, SelectNode]


# Mapping of node names to classes
NODE_CLASS_MAPPINGS = {node_class.__name__: node_class for node_class in node_classes}

# Mapping of node names to friendly display names
NODE_DISPLAY_NAME_MAPPINGS = {node_class.__name__: node_class.__name__.replace("Node", " Node") for node_class in node_classes}