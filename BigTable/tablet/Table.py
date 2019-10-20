class ColumnFamily:
    def __init__(self, name, columns):
        """ Definition for a column family containing list of columns
        
        Args:
            name (str): name of column family
            columns (list[str]): list of column names
        """
        self.columns = columns
        self.name = name

class Table:
    def __init__(self, name, columnFamilies):
        """ Definition of a table. Consists of a name and a list of ColumnFamily objects.
        
        Args:
            name (str): Table name
            columnFamilies (List[ColumnFamily]): List of column families
        """
        self.name = name
        self.columnFamilies = columnFamilies
    
    def getAsJSON(self):
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
        return resp