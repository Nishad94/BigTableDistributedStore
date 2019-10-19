class MetadataManager:
    def __init__(self, metadataPath):
        self.tableTabletMap = {}
        self.tableIdx = {}
        self.metadataPath = metadataPath
    
    def getTables(self):
        return list(self.tableIdx.values())
    
    def getTableInfo(self, tableName):
        return self.tableIdx[tableName]
    
    def addTable(self, table):
        self.tableIdx[table.name] = table
        if self.tableTabletMap[table.name] == None:
            self.tableTabletMap[table.name] = []
    
    def addTablet(self, tablet):
        self.addTable(tablet.tableName)
        tablets_for_table = self.tableTabletMap[tablet.tableName]
        tablets_for_table.append(tablet)
    
    def getTablet(self,tableName,rowKey):
        pass