class WAL:
    def __init__(self, path, tableName):
        """ Write Ahead Log: used for failure recovery
        
        Args:
            path (str): Path to store WAL
            tableName (str): Name of the table
        """
        self.path = path
        self.tableName = tableName
        self.log = ""
    
    def append(self, query):
        self.log += query
    
    def save(self):
        with open(self.path + "_" + self.tableName,'a') as f:
            f.write(self.log)
    
    def appendAddQuery(self):
        pass
    
    def appendDeleteQuery(self):
        pass