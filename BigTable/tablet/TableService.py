from MetadataManager import MetadataManager
from WAL import WAL
from Tablet import Tablet

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
        wal = WAL(self.walPath, table)
        self.WALIdx[table.name] = wal
        tablet = self.createTablet(table)
        self.metaMgr.addTable(table,tablet)
    
    def deleteTable(self,tableName):
        self.metaMgr.removeTable(tableName)
        self.WALIdx[tableName].delete()
        del self.WALIdx[tableName]
    
    def listTables(self):
        tables = self.metaMgr.getTables()
        table_names = [t.name for t in tables]
        return table_names

    def getTableInfo(self, tableName):
        table = self.metaMgr.getTableInfo(tableName)
        return table
    
    def getEntry(self, tableName, rowKey, colFam = None, col = None):
        tablet = self.getRelevantTablet(tableName, rowKey)
        data = tablet.getRow(rowKey,colFam,col)
        return data
    
    def addNewEntry(self, tableName, rowKey, colFam, col, content):
        self.WALIdx[tableName].appendAddQuery(tableName, rowKey, colFam, col, content)
        self.WALIdx[tableName].save()
        tablet = self.metaMgr.getRelevantTablet(tableName, rowKey)
        if tablet.isFull() is False:
            self.splitTablet(tablet)
            tablet = self.metaMgr.getRelevantTablet(tableName, rowKey)
        tablet.addRow(rowKey, colFam, col, content)
    
    def splitTablet(self):
        pass

    def createTablet(self, table):
        curr_tablets = self.metaMgr.getAllTablets(table.name)
        tablet = Tablet(len(curr_tablets), 0, table.name, 'a', 'z', self.ssTablePath)
        return tablet