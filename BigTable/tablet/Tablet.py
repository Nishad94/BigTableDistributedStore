import collections
import json
import os

class MemTable:
    def __init__(self, capacity, tablet, maxCellCopies):
        self.rowEntries = {}
        self.capacity = capacity
        self.tablet = tablet
        self.maxCellCopies = maxCellCopies
    
    def getCurrentSize(self):
        return len(self.rowEntries)
    
    def clear(self):
        self.rowEntries = {}
    
    def getRow(self, rowKey, columnFamily=None, columnKey=None):
        if rowKey not in self.rowEntries:
            return None
        if columnFamily and columnKey:
            return self.rowEntries[rowKey][columnFamily][columnKey]
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
        serialized_sst = ""
        for key, item in self.sst.items():
            s = json.dumps(item) + "\n"
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
            row = self.sst[rowKey][columnFamily][columnKey]
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
            id ([type]): [description]
            serverId ([type]): [description]
            tableName ([type]): [description]
            startKey ([type]): [description]
            endKey ([type]): [description]
            ssTablePath ([type]): [description]
            loadFromJson (boolean): Load Tablet from serialized string
            memTableCapacity (int, optional): [description]. Defaults to 10.
            ssTables ([type], optional): [description]. Defaults to None.
            memTable ([type], optional): [description]. Defaults to None.
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
        return self.startKey <= rowKey[:len(self.startKey)] and self.endKey >= rowKey[:len(self.endKey)]
    
    def delete(self):
        self.memTable.clear()
        for sst in self.ssTables:
            sst.delete()