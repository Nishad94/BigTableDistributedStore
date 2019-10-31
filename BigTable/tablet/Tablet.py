import collections
import json
import os

class MemTable:
    """An in memory data structure for storing most recent insertions/deletions in this tablet. After it reaches capacity it is dumped
    to disk as an SST and then cleare. 
    """

    def __init__(self, capacity, tablet, maxCellCopies):
        self.rowEntries = {}
        self.capacity = capacity
        self.tablet = tablet
        self.maxCellCopies = maxCellCopies
    
    def changeCapacity(self, newVal):
        self.capacity = newVal
        if len(self.rowEntries) > newVal:
            self.createSSTable()
    
    def getCurrentSize(self):
        return len(self.rowEntries)
    
    def clear(self):
        self.rowEntries = {}
    
    def getRow(self, rowKey, columnFamily=None, columnKey=None):
        if rowKey not in self.rowEntries:
            return None
        if columnFamily and columnKey:
            if columnFamily in self.rowEntries[rowKey] and columnKey in self.rowEntries[rowKey][columnFamily]:
                return self.rowEntries[rowKey][columnFamily][columnKey]
            else:
                return None
        else:
            return self.rowEntries[rowKey]
    
    def addRow(self, rowKey, columnFamily, columnKey, cellContent, time_val):
        created_sst = False
        if len(self.rowEntries) == self.capacity:
            self.createSSTable()
            self.clear()
            created_sst = True
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
        cells.append((cellContent,float(time_val)))
        self.rowEntries[rowKey] = entry
        return created_sst
    
    def deleteRow(self, rowKey):
        del self.rowEntries[rowKey]

    def removeOldestCell(self, rowKey, columnFamily, columnKey):
        cells = self.getRow(rowKey,columnFamily,columnKey)
        cells.sort(key = lambda x: x[1],reverse=True)
        cells = cells[:-1]
        return cells

    def createSSTable(self):
        sst = SSTable(len(self.tablet.ssTables),self,self.tablet)
        self.tablet.addSST(sst)


class SSTable:
    """Stores the memtable on disk in sorted order. Also maintains an index at the EOF for fast retrieval of keys from specific offsets.
    """

    def __init__(self, id, memTable, tablet):
        self.id = id
        self.tablet = tablet
        self.fileName = self.tablet.ssTablePath + "/" + str(self.tablet.id) + "_" + str(self.id) + ".sst"
        self.sst = {}
        self.sstIndex = {}
        if memTable:
            self.createSST(memTable)
            self.dumpToDisk()
        else:   
            self.readIndexFromDisk()
            self.readFromDisk()
    
    def isLoadedInMemory(self):
        return self.sst is not None
    
    def createSST(self, memTable):
        self.sst = collections.OrderedDict(sorted(memTable.rowEntries.items()))
        
    def serializeSSTAndCreateIndex(self):
        """Each entry's value is new-line seperated in the string. The key is stored in the index and is mapped to the starting offset
        of its value in the serialized string.
        
        Returns:
            serialized_sst: the sst as string
        """
        serialized_sst = ""
        for key, val in self.sst.items():
            s = json.dumps(val) + "\n"
            self.sstIndex[key] = len(serialized_sst)
            serialized_sst += s
        return serialized_sst

    def serializeIndex(self):
        return json.dumps(self.sstIndex)

    def dumpToDisk(self):
        serialised_dump = self.serializeSSTAndCreateIndex()
        serialized_idx = self.serializeIndex()
        with open(self.fileName,"w") as f:
            f.write(serialised_dump+serialized_idx)
    
    def readIndexFromDisk(self):
        """From the EOF seek backwards until the first newline is found. The newline indicates
        the end_idx of the stored vals, and beginning of the index.
        """
        with open(self.fileName) as fp:
            p = fp.seek(0,2)
            while True:
                if fp.read(1) == "\n":
                    break
                p -= 1
                fp.seek(p,0)
            p += 1
            fp.seek(p,0)
            self.sstIndex = json.loads(fp.readline())
    
    def readFromDisk(self):
        """Read value of key from offset in the file
        """
        with open(self.fileName) as fp:
            for key,item in self.sstIndex.items():
                fp.seek(item,0)
                self.sst[key] = json.loads(fp.readline()[:-1] )
    
    def clearFromMemory(self):
        self.sst = {}
    
    def clearIndexFromMemory(self):
        self.sstIndex = {}
    
    def delete(self):
        self.clearFromMemory()
        self.clearIndexFromMemory()
        os.remove(self.fileName)

    def search(self, rowKey, columnFamily=None, columnKey=None):
        if self.sstIndex is None:
            self.readIndexFromDisk()
        if rowKey not in self.sstIndex:
            return None
        self.readFromDisk()
        
        if columnFamily and columnKey:
            if columnFamily in self.sst[rowKey] and columnKey in self.sst[rowKey][columnFamily]:
                row = self.sst[rowKey][columnFamily][columnKey]
            else:
                row = None
        else:
            row = self.sst[rowKey]
        self.clearFromMemory()
        return row


class Tablet:
    def __init__(self, id, serverId, tableName, startKey, endKey, ssTablePath, loadFromJson=None, maxCellCopies = 5, tabletCapacity = 100, memTableCapacity = 100):
        """ A tablet is a way to horizontally shard data in a table (basically a list of rows). Thus each tablet is responsible for a range
        of row keys (lexicographic increasing). It consists of an in memory table which it dumps to disk periodically after it 
        reaches its capacity. The on-disk counterpart of the in-mem table is the SSTable. 
        
        Args:
            id (str): Unique id for this tablet. Currently using the current size of parent table in terms of tablets
            serverId (str): Tablet server id. Each server might be responsible for multiple tablets
            tableName (str): Name of the table
            startKey (str): Start key
            endKey (str): End key
            ssTablePath (str): Path to store SSTs for this tablet on disk
            loadFromJson (boolean): If true, load tablet from serialized string
            maxCellCopies (int, optional): Dictates how many versions of a cell's history must be kept,  defaults to 5
            tabletCapacity (int, optional): Max elements in tablet. Beyond this capacity the tablet splits into 2,  defaults to 100
            memTableCapacity (int, optional): Beyond this capacity the memTable is dumped to disk as an SST, defaults to 100
        """
        self.id = id
        self.serverId = serverId
        self.tableName = tableName
        self.startKey = startKey
        self.endKey = endKey
        self.maxCellCopies = maxCellCopies
        self.memTableCapacity = memTableCapacity
        self.tabletCapacity = tabletCapacity
        self.ssTables = []
        self.ssTablePath = ssTablePath
        self.memTable = None
        self.currentSize = 0
        self.memTable = MemTable(memTableCapacity, self, maxCellCopies)
        if loadFromJson is not None:
            self.loadFromJson(loadFromJson)

    def changeMemtableCapacity(self, newVal):
        self.memTable.changeCapacity(newVal)
    
    def serialize(self):
        s = {
            "id": self.id,
            "serverId": self.serverId,
            "tableName": self.tableName,
            "startKey": self.startKey ,
            "endKey": self.endKey,
            "maxCellCopies": self.maxCellCopies,
            "memTableCapacity": self.memTableCapacity,
            "tabletCapacity": self.tabletCapacity,
            "ssTablePath": self.ssTablePath,
        }
        s["sstIds"] = [sst.id for sst in self.ssTables]
        return json.dumps(s)
    
    def loadFromJson(self, JsonStr):
        s_dict = json.loads(JsonStr)
        self.__init__(s_dict["id"], s_dict['serverId'], s_dict['tableName'], s_dict['startKey'], s_dict['endKey'], \
            s_dict['ssTablePath'], None, s_dict['maxCellCopies'], s_dict['tabletCapacity'], s_dict['memTableCapacity'])
        for sst_id in s_dict["sstIds"]:
            self.ssTables.append(SSTable(sst_id, None, self))
    
    def addSST(self,sst):
        self.ssTables.append(sst)
    
    def getCurrentSize(self):
        return self.currentSize
    
    def isFull(self):
        return self.getCurrentSize() >= self.tabletCapacity
    
    def createSSTable(self):
        ssTable = SSTable(len(self.ssTables), self.memTable, self.id)
        self.ssTables.append(ssTable)
        ssTable.dumpToDisk(self.ssTablePath)
        self.memTable.clear()
    
    def getRow(self, rowKey, columnFamily=None, columnKey=None):
        row = self.memTable.getRow(rowKey, columnFamily, columnKey)
        if row is None:
            for sst in self.ssTables:
                row = sst.search(rowKey,columnFamily,columnKey)
                if row is not None:
                    return row
            return None
        else:
            return row
    
    def getAllRows(self, columnFamily, columnKey):
        allEntries = self.memTable.rowEntries
        resp = {}
        for entry in allEntries:
            resp[entry] = allEntries[entry][columnFamily][columnKey]
        for sst in self.ssTables:
            sst.readFromDisk()
            for rowKey in sst.sst:
                resp[rowKey] = sst.sst[rowKey][columnFamily][columnKey]
            sst.clearFromMemory()
        return resp
    
    def getRowRange(self, rowKeyStart, rowKeyEnd, columnFamily, columnKey):
        rows = self.getAllRows(columnFamily,columnKey)
        resp = {}
        for r in rows:
            if r[:len(rowKeyStart)] >= rowKeyStart and r[:len(rowKeyEnd)] <= rowKeyEnd:
                resp[r] = rows[r]
        return resp

    def addRow(self, rowKey, columnFamily, columnKey, cellContent, time_val):
        return self.memTable.addRow(rowKey, columnFamily, columnKey, cellContent, time_val)
    
    def intersect(self, rowKey):
        return self.startKey <= str(rowKey)[:len(self.startKey)] and self.endKey >= str(rowKey)[:len(self.endKey)]
    
    def delete(self):
        self.memTable.clear()
        for sst in self.ssTables:
            sst.delete()