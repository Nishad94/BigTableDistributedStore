class MemTable:
    def __init__(self, capacity, tabletId, maxCellCopies):
        self.rowEntries = {}
        self.capacity = capacity
        self.tabletId = tabletId
        self.maxCellCopies = maxCellCopies
    
    def getCurrentSize(self):
        return len(self.rowEntries)
    
    def clear(self):
        self.rowEntries = {}
    
    def getRow(self, rowKey, columnFamily=None, columnKey=None):
        if columnFamily and columnKey:
            return self.rowEntries[rowKey][columnFamily][columnKey]
        else:
            return self.rowEntries[rowKey]
    
    def addRow(self, rowKey, columnFamily, columnKey, cellContent, time_val):
        if len(self.rowEntries) > self.capacity:
            return False

        if rowKey not in self.rowEntries:
            entry = {}
        else:
            entry = self.rowEntries[rowKey]
        if columnFamily not in entry:
            entry[columnFamily] = {}
        if columnKey in entry[columnFamily]:
            cells = entry[columnFamily][columnKey]
        else:
            cells = []
            entry[columnFamily][columnKey] = cells
        if len(cells) == self.maxCellCopies:
            cells = self.removeOldestCell(rowKey, columnFamily, columnKey)
            entry[columnFamily][columnKey] = cells
        cells.append((cellContent,time_val))
        self.rowEntries[rowKey] = entry
        return True
    
    def deleteRow(self, rowKey):
        del self.rowEntries[rowKey]

    def removeOldestCell(self, rowKey, columnFamily, columnKey):
        cells = self.getRow(rowKey,columnFamily,columnKey)
        cells.sort(key = lambda x: x[1])
        cells = cells[:-1]
        return cells


class SSTable:
    def __init__(self, id, memTable, tabletId):
        self.id = id
        self.memTable = memTable
        self.tabletId = tabletId
    
    def dumpToDisk():
        pass
    
    def clear():
        pass


class Tablet:
    def __init__(self, id, serverId, tableName, startKey, endKey, ssTablePath, maxCellCopies = 5, tabletCapacity = 100, memTableCapacity = 10, ssTables = None, memTable = None):
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
        self.tabletCapacity = tabletCapacity
        self.ssTables = ssTables
        self.ssTablePath = ssTablePath
        self.memTable = memTable
        self.currentSize = 0
        if ssTables is None:
            self.ssTables = []
        if memTable is None:
            self.memTable = MemTable(memTableCapacity, self.id, maxCellCopies)
    
    def getCurrentSize(self):
        return self.currentSize
    
    def isFull(self):
        return self.getCurrentSize() >= self.tabletCapacity
    
    def createSSTable(self):
        ssTable = SSTable(len(self.ssTables), self.memTable, self.id)
        sefl.ssTables.append(ssTable)
        ssTable.dumpToDisk(self.ssTablePath)
        self.memTable.clear()
    
    def getRow(self, rowKey, columnFamily=None, columnKey=None):
        return self.memTable.getRow(rowKey, columnFamily, columnKey)
    
    def getAllRows(self, columnFamily, columnKey):
        allEntries = self.memTable.rowEntries
        resp = {}
        for entry in allEntries:
            resp[entry] = allEntries[entry][columnFamily][columnKey]
        return resp
    
    def getRowRange(self, rowKeyStart, rowKeyEnd, columnFamily, columnKey):
        rows = self.getAllRows(columnFamily,columnKey)
        resp = {}
        for r in rows:
            if r[:len(rowKeyStart)] >= rowKeyStart and r[:len(rowKeyEnd)] <= rowKeyEnd:
                resp[r] = rows[r]
        return resp


    def addRow(self, rowKey, columnFamily, columnKey, cellContent, time_val):
        self.memTable.addRow(rowKey, columnFamily, columnKey, cellContent, time_val)
    
    def intersect(self, rowKey):
        return self.startKey <= rowKey[:len(self.startKey)] and self.endKey >= rowKey[:len(self.endKey)]
    
    def delete(self):
        self.memTable.clear()
        for sst in self.ssTables:
            sst.clear()