class TableInformation:
    def __init__(self, conn):
        self.conn = conn
        self.table_info = {}
        self.fields = {}
    def load_table_info(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'")
        tables = [row.table_name for row in cursor.fetchall()]
        for table in tables:
            cursor.execute(f"SELECT * FROM {table}")
            columns = {column[0]: column[1] for column in cursor.description}
           # print(columns)

            self.table_info[table] = columns
        self.fields = self.table_info['txt2img']