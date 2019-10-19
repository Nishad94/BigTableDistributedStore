class ColumnFamily:
    def __init__(self,name,columns):
        """ Definition for a column family containing list of columns
        
        Args:
            name (str): name of column family
            columns (list[str]): list of column names
        """
        self.columns = columns

class Table:
    def __init__(self, name, columnFamilies):
        """ Definition of a table. Consists of a name and a list of ColumnFamily objects.
        
        Args:
            name (str): Table name
            columnFamilies (List[ColumnFamily]): List of column families
        """
        self.name = name
        self.columnFamilies = columnFamilies