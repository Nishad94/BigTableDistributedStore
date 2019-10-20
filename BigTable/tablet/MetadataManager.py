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
    
    def dumpToDisk(self):
        pass
    
    def getTables(self):
        return list(self.tableIdx.values())
    
    def getTable(self, tableName):
        return self.tableIdx[tableName]
    
    def addTable(self, table, tablet):
        self.tableIdx[table.name] = table
        self.tableTabletMap[table.name] = [tablet]
    
    def addTablet(self, tablet):
        tablets_for_table = self.tableTabletMap[tablet.tableName]
        tablets_for_table.append(tablet)
    
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