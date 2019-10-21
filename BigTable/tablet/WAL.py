import os

class WAL:
    def __init__(self, path, tableName, loadFromJson=None):
        """ Write Ahead Log: used for failure recovery
        Format:
            {QUERY_TYPE=[INSERT/DELETE]} {row_key1} [{col_fam} {col} {content}]
            .
            .
            .
            {QUERY_TYPE=[INSERT/DELETE]} {row_keyN} [{col_fam} {col} {content}]
        
        Args:
            path (str): Path to store WAL
            table (Table): Table object
            loadFromJson (str,optional): WAL serialized as JSON
        """
        self.path = path
        self.tableName = tableName
        self.log = ""
        self.fileName = path + "/" + tableName + ".wal"
        if loadFromJson:
            self.loadFromJson(loadFromJson)
        else:
            self.save()
    
    def loadFromJson(self, s):
        self.log = s
    
    def save(self):
        with open(self.fileName,'a') as f:
            f.write(self.log)
            self.log = ""
    
    def appendAddQuery(self, rowKey, colFam, col, content, time_val):
        self.log += f"INSERT,{rowKey},{colFam},{col},{content},{time_val}\n"
        self.save()
    
    def appendDeleteQuery(self, rowKey):
        self.log += f"DELETE,{rowKey},{colFam},{col},{content},{time_val}\n"
        self.save()
    
    def delete(self):
        os.remove(self.fileName)
    
    def replay(self, tableService):
        queries = self.log.split("\n")
        queries.reverse()

        for q in queries:
            parts = q.split(",")
            if len(parts) == 0:
                continue
            if parts[0] == "INSERT":
                cells = tableService.getEntry(self.tableName, parts[1], parts[2], parts[3])
                found = False
                if cells is not None:
                    for c in cells:
                        if c[0] == parts[4] and c[1] == float(parts[5]):
                            found = True
                            break
                if found is False:
                    tableService.addNewEntry(self.tableName, parts[1], parts[2], parts[3], parts[4], parts[5],False)