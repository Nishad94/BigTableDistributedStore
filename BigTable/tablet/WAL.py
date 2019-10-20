import os

class WAL:
    def __init__(self, path, table):
        """ Write Ahead Log: used for failure recovery
        
        Args:
            path (str): Path to store WAL
            table (Table): Table object
        """
        self.path = path
        self.table = table
        self.log = ""
        self.fileName = path + "/" + table.name + ".wal"
        self.createOnDisk()
    
    def createOnDisk(self):
        """ Stores the WAL on disk.
        Format:
            {TABLE_NAME}
            {Col_Fam1}[,{Col_Fam2},...{Col_FamN}]
            {Col_Fam1:Col1}[,{Col_Fam1:Col2},{Col_Fam1:Col2}...]
            [{Col_Fam2:Col1}[,{Col_Fam2:Col2},{Col_Fam2:Col2}...]]
            {QUERY_TYPE=[INSERT/DELETE]} {row_key1} [{col_fam} {col} {content}]
            .
            .
            .
            {QUERY_TYPE=[INSERT/DELETE]} {row_keyN} [{col_fam} {col} {content}]
        """
        with open(self.fileName,'w') as f:
            tableInfo = self.serializeTableInfo()
            f.write(tableInfo)
        
    def serializeTableInfo(self):
        s = ""
        s += self.table.name + "\n"
        for cf in self.table.columnFamilies:
                s += cf.name + ","
        s = s[:-1] + "\n"
        for cf in self.table.columnFamilies:
            for c in cf.columns:
                s += c + ","
            s = s[:-1] + "\n"
        return s
    
    def save(self):
        with open(self.fileName,'a') as f:
            f.write(self.log)
            self.log = ""
    
    def appendAddQuery(self, rowKey, colFam, col, content, time_val):
        pass
    
    def appendDeleteQuery(self, rowKey):
        pass
    
    def delete(self):
        os.remove(self.fileName)