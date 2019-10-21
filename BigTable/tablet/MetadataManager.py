import json
import os
from Tablet import Tablet
from Table import Table

class MetadataManager:
    def __init__(self, metadataPath):
        """ This class is responsible for handling stuff related to the METADATA file which contains mapping of
        table names to their tablets. Provides relevant tablet for a particular row key.
        
        Args:
            metadataPath (str): METADATA file path
        """
        self.tableTabletMap = {}
        self.tableIdx = {}
        self.metadataPath = metadataPath
        self.table_meta = self.metadataPath + "/" + "meta.table"
        self.tablet_meta = self.metadataPath + "/" + "meta.tablet"
        if os.path.exists(self.table_meta) and os.path.exists(self.tablet_meta):
            self.loadFromDisk()

    def loadFromDisk(self):
        with open(self.tablet_meta) as f:
            tablet_map = json.loads(f.read())
            for tName, vals in tablet_map.items():
                self.tableTabletMap[tName] = []
                for val in vals:
                    self.tableTabletMap[tName].append(Tablet(None,None,None,None,None,None,val))
        
        with open(self.table_meta) as f:
            table_map = json.loads(f.read())
            for tName, val in table_map.items():
                self.tableIdx[tName] = Table(None,None,val)

    def dumpToDisk(self):
        serialized_dict = {}
        for tableName, tablets in self.tableTabletMap.items():
            serialized_dict[tableName] = []
            for tablet in tablets:
                serialized_dict[tableName].append(tablet.serialize())
        s = json.dumps(serialized_dict)
        with open(self.tablet_meta,"w") as f:
            f.write(s)
        
        serialized_dict = {}
        for tableName, table in self.tableIdx.items():
            serialized_dict[tableName] = table.getAsJson()
        s = json.dumps(serialized_dict)
        with open(self.table_meta,"w") as f:
            f.write(s)
    
    def getTables(self):
        return list(self.tableIdx.values())
    
    def getTable(self, tableName):
        return self.tableIdx[tableName]
    
    def addTable(self, table, tablet):
        self.tableIdx[table.name] = table
        self.tableTabletMap[table.name] = [tablet]
        self.dumpToDisk()
    
    def addTablet(self, tablet):
        tablets_for_table = self.tableTabletMap[tablet.tableName]
        tablets_for_table.append(tablet)
        self.metaMgr.dumpToDisk()
    
    def getRelevantTablet(self,tableName,rowKey):
        tablets = self.getAllTablets(tableName)
        for t in tablets:
            if t.intersect(rowKey) is True:
                return t
        raise "ERROR: No relevant tablet!"
        
    def getAllTablets(self, tableName):
        if tableName in self.tableTabletMap:
            return self.tableTabletMap[tableName]
        return []
    
    def removeTable(self, tableName):
        tablets = self.getAllTablets(tableName)
        for t in tablets:
            t.delete()
        del self.tableTabletMap[tableName]
        del self.tableIdx[tableName]
        self.dumpToDisk()
    
    def delete(self):
        os.remove(self.table_meta)
        os.remove(self.tablet_meta)