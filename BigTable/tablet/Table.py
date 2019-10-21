import json

class ColumnFamily:
    def __init__(self, name, columns):
        """ Definition for a column family containing list of columns
        
        Args:
            name (str): name of column family
            columns (list[str]): list of column names
            loadFromJson (str, optional: Load serialized ColumnFamily
        """
        self.columns = columns
        self.name = name


class Table:
    def __init__(self, name, columnFamilies, loadFromJson=None):
        """ Definition of a table. Consists of a name and a list of ColumnFamily objects.
        
        Args:
            name (str): Table name // Ignored if loadFromJson is not None
            columnFamilies (List[ColumnFamily]): List of column families // Ignored if loadFromJson is not None
            loadFromJson (str, optional): Serialized JSON str representing object.
        """
        self.name = name
        self.columnFamilies = columnFamilies
        if loadFromJson:
            self.loadFromJson(loadFromJson)
    
    def getAsJson(self):
        resp = {}
        resp["name"] = self.name
        resp["column_families"] = []
        for cf in self.columnFamilies:
            cf_resp = {}
            cf_resp["column_family_key"] = cf.name
            cf_resp["columns"] = []
            for c in cf.columns:
                cf_resp["columns"].append(c)
            resp["column_families"].append(cf_resp)
        return json.dumps(resp)

    def loadFromJson(self, jsonStr):
        resp = json.loads(jsonStr)
        self.name = resp["name"]
        self.columnFamilies = []
        for cf in self.columnFamilies:
            cf_name = cf["column_family_key"]
            cf_cols = cf["columns"]
            self.columnFamilies.append(ColumnFamily(cf_name,cf_cols))