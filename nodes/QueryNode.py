class QueryNode:
    def __init__(self):
        self.CATEGORY = "LexNode.MSSQL"
        self.FUNCTION = "execute_query"
        self.RETURN_TYPES = ("Tuple", )
        self.RETURN_NAMES = ("Query Results", )

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "query": ("STRING", {"default": ""})
            },
        }

    def execute_query(self, **kwargs):
        global conn
        cursor = conn.cursor()

        sql = kwargs.get('query')
        cursor.execute(sql)
        results = cursor.fetchall()
        all_rows = []

        for result in results:
            all_rows.append(result)

        return tuple(all_rows),

    RETURN_TYPES = ("Tuple", )
    RETURN_NAMES = ("Query Results", )

NODE_CLASS_MAPPINGS = {
    "QueryNode": QueryNode
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "QueryNode": "Sql Query Node"
}