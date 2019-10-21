from MetadataManager import MetadataManager
from WAL import WAL
from Tablet import Tablet
import os

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
        self.loadWAL()
    
    def loadWAL(self):
        tables = self.metaMgr.getTables()
        for t in tables:
            if os.path.exists(self.walPath + "/" + t.name + ".wal"):
                with open(self.walPath + "/" + t.name + ".wal") as f:
                    self.WALIdx[t.name] = WAL(self.walPath,t.name,f.read())
                    self.WALIdx[t.name].replay(self)
    
    def createTable(self, table, maxCellCopies = 5, tabletCapacity = 100, memTableCapacity = 100):
        wal = WAL(self.walPath, table.name)
        self.WALIdx[table.name] = wal
        tablet = self.createTablet(table,maxCellCopies,tabletCapacity,memTableCapacity)
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
        table = self.metaMgr.getTable(tableName)
        return table
    
    def getEntry(self, tableName, rowKey, colFam = None, col = None):
        tablet = self.metaMgr.getRelevantTablet(tableName, rowKey)
        data = tablet.getRow(rowKey,colFam,col)
        return data
    
    def getEntryRange(self, tableName, rowKeyStart, rowKeyEnd, colFam, col):
        tablet = self.metaMgr.getAllTablets(tableName)
        data = {}
        for t in tablet:
            curr = t.getRowRange(rowKeyStart,rowKeyEnd,colFam,col)
            if curr is not None and len(curr) > 0:
                data = dict(list(data.items()) + list(curr.items()))
        return data
    
    def addNewEntry(self,tableName, rowKey, colFam, col, content, time_val, walWrite=True):
        if walWrite is True:
            self.WALIdx[tableName].appendAddQuery(rowKey, colFam, col, content, time_val)
            self.WALIdx[tableName].save()
        tablet = self.metaMgr.getRelevantTablet(tableName, rowKey)
        if tablet.isFull() is True:
            self.splitTablet(tablet)
            tablet = self.metaMgr.getRelevantTablet(tableName, rowKey)
        meta_change = tablet.addRow(rowKey, colFam, col, content, time_val)
        if meta_change:
            self.metaMgr.dumpToDisk()
    
    def changeMaxCells(self,tableName,newVal):
        tablets = self.metaMgr.getAllTablets(tableName)
        for t in tablets:
            t.changeMaxCellCopies(newVal)
    
    def splitTablet(self,tablet):
        pass

    def createTablet(self, table, maxCellCopies, tabletCapacity, memTableCapacity):
        curr_tablets = self.metaMgr.getAllTablets(table.name)
        tablet = Tablet(len(curr_tablets), 0, table.name, 'a', 'z', self.ssTablePath, None, maxCellCopies, tabletCapacity, memTableCapacity)
        return tablet