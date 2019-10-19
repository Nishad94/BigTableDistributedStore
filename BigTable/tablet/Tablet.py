class MemTable:
    def __init__(self, capacity, tabletId):
        self.rowEntries = {}
        self.capacity = capacity
        self.tabletId = tabletId
    
    def clear(self):
        self.rowEntries = {}
    
    def getRow(self, rowKey, columnFamily=None,columnKey=None):
        return self.rowEntries[rowKey]
    
    def addRow(self, rowKey, columnFamily, columnKey, cellContent):
        if len(self.rowEntries) > self.capacity:
            return False

        if rowKey not in self.rowEntries:
            entry = {}
            entry[columnFamily] = {}
            entry[columnFamily][columnKey] = cellContent
            self.rowEntries[rowKey] = rowKey
            return True
    
    def deleteRow(self, rowKey):
        pass


class SSTable:
    def __init__(self, id, memTable, tabletId):
        self.id = id
        self.memTable = memTable
        self.tabletId = tabletId
    
    def dumpToDisk():
        pass


class Tablet:
    def __init__(self, id, serverId, tableName, startKey, endKey, ssTablePath, memTableCapacity = 10, ssTables = None, memTable = None):
        """ A tablet is a way to horizontally shard data in a table (basically a list of rows). Thus each tablet is responsible for a range
        of row keys (lexicographic increasing). It consists of an in memory table which it dumps to disk periodically after it 
        reaches its capacity. The on-disk counterpart of the in-mem table is the SSTable. 
        
        Args:
            id ([type]): [description]
            serverId ([type]): [description]
            tableName ([type]): [description]
            startKey ([type]): [description]
            endKey ([type]): [description]
            ssTablePath ([type]): [description]
            memTableCapacity (int, optional): [description]. Defaults to 10.
            ssTables ([type], optional): [description]. Defaults to None.
            memTable ([type], optional): [description]. Defaults to None.
        """
        self.id = id
        self.serverId = serverId
        self.tableName = tableName
        self.startKey = startKey
        self.endKey = endKey
        self.ssTables = ssTables
        self.ssTablePath = ssTablePath
        self.memTable = memTable
        if ssTables is None:
            self.ssTables = []
        if memTable is None:
            self.memTable = MemTable(memTableCapacity, self.id)
    
    def createSSTable(self):
        ssTable = SSTable(len(self.ssTables), self.memTable, self.id)
        sefl.ssTables.append(ssTable)
        ssTable.dumpToDisk(self.ssTablePath)
        self.memTable.clear()
    
    def getRow(self,rowKey):
        pass
    
    def addRow(self, rowKey, columnFamily, columnKey, cellContent):
        self.memTable.addRow(rowKey, columnFamily, columnKey, cellContent)