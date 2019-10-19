import MetadataManager
import WAL

class TableService:
    def __init__(self, metadataPath, ssTablePath, walPath):
        """ This is the main data serving service. It will be used by clients for all kinds of queries.
        
        Args:
            metadataPath (str): Path to store the METADATA file. 
            ssTablePath (str): Path to store SSTables for each Tablet
            walPath (str): Path to store Write Ahead Log for each table
        """
        self.metaMgr = MetadataManager(metadataPath)
        self.metadataPath = metadataPath
        self.ssTablePath = ssTablePath
        self.walPath = walPath
        self.WALIdx = {}
    
    def createTable(self, table):
        wal = WAL(self.walPath, table.tableName)
        wal.save()
        self.WALIdx[table.tableName] = wal
        tablet = self.createTablet(table)
        self.metaMgr.addTable(table,tablet)
    
    def listTables(self):
        tables = self.metaMgr.getTables()
        table_names = [t.name for t in tables]
        return table_names

    def getTableInfo(self, tableName):
        table = self.metaMgr.getTableInfo(tableName)
        return table
    
    def addNewEntry(self, tableName, rowKey, colFam, col, content):
        self.WALIdx[tableName].append(self.convertQueryToString(tableName, rowKey, colFam, col, content))
        self.WALIdx[tableName].save()
        tablet = self.metaMgr.getRelevantTablet(tableName, rowKey)
        tablet.addRow(rowKey, colFam, col, content)
    
    def createTablet(self, table):
        curr_tablets = self.metaMgr.getAllTablets(table.tableName)
        tablet = Tablet(len(curr_tablets), 0, table.tableName, 'a', 'z', self.ssTablePath)
        return tablet
    
    def convertQueryToString(self, tableName, rowKey, colFam, col, content):
        return ""