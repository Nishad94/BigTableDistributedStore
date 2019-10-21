from TableService import TableService
import Table

tableService = None 

def init_service(metadataPath, ssTablePath, walPath):
    global tableService
    tableService = TableService(metadataPath, ssTablePath, walPath)

def createTable(tableName):
    colFams = ["cf1","cf2"]
    cols = [["c1","c2"],["c1","c2"]]
    colFam1 = Table.ColumnFamily(colFams[0],cols[0])
    colFam2 = Table.ColumnFamily(colFams[1],cols[1])
    table = Table.Table(tableName,[colFam1,colFam2])
    return table

def test_listTables():
    table = createTable("testTable")
    tableService.createTable(table)
    table2 = createTable("testTable2")
    tableService.createTable(table2)
    tables = tableService.listTables()
    print(tables)
    if table.name not in tables or table2.name not in tables:
        raise Exception("Error in creating table: Table not found")
    tableService.deleteTable(table.name)
    tableService.deleteTable(table2.name)

def test_createTable():
    tableName = "testTable"
    table = createTable(tableName)

    tableService.createTable(table)
    tables = tableService.listTables()
    if tables[0] != tableName:
        raise Exception("Error in creating table: Table not found")
    tableService.deleteTable(tableName)

def test_deleteTable():
    tableName = "testTable"
    table = createTable(tableName)
    tableService.createTable(table)

    tableService.deleteTable(tableName)
    # TODO: check if sst + wal files deleted
    tables = tableService.listTables()
    if tableName in tables:
        raise Exception("Error: Found deleted tables in metadata manager!")
    
def test_getTableInfo():
    tableName = "testTable"
    table = createTable(tableName)
    tableService.createTable(table)
    t_info = tableService.getTableInfo(tableName)
    print(t_info.getAsJson())
    tableService.deleteTable(tableName)

def test_insertEntry():
    tableName = "testTable"
    table = createTable(tableName)
    tableService.createTable(table)
    tableService.addNewEntry(tableName,"first","cf1","c1","Hello!",123.0)
    tableService.addNewEntry(tableName,"first","cf1","c1","Hello!",124.0)
    cells = tableService.getEntry(tableName,"first","cf1","c1")
    print(cells)
    found = False
    for c in cells:
        if c[0] == "Hello!":
            found = True
            break
    if not found:
        raise "Error in retrieval or insertion"

    tableService.addNewEntry(tableName,"first","cf2","c2","Hello2!",123.0)
    cells = tableService.getEntry(tableName,"first","cf2","c2")
    found = False
    for c in cells:
        if c[0] == "Hello2!":
            found = True
            break
    if not found:
        raise "Error in retrieval or insertion"
    
    tableService.addNewEntry(tableName,"second","cf2","c2","Hello3!",123.0)
    cells = tableService.getEntry(tableName,"second","cf2","c2")
    found = False
    for c in cells:
        if c[0] == "Hello3!":
            found = True
            break
    if not found:
        raise "Error in retrieval or insertion"
    tableService.deleteTable(tableName)
    
def test_getRowRange():
    tableName = "testTable"
    table = createTable(tableName)
    tableService.createTable(table)
    tableService.addNewEntry(tableName,"aaa","cf1","c1","Hello!",123.0)
    tableService.addNewEntry(tableName,"aab","cf1","c1","Hello!",124.0)
    tableService.addNewEntry(tableName,"ab","cf1","c1","Hello!",124.0)
    tableService.addNewEntry(tableName,"cd","cf1","c1","Hello!",124.0)
    tableService.addNewEntry(tableName,"dd","cf1","c1","Hello!",124.0)
    resp = tableService.getEntryRange(tableName,"aaa","d","cf1","c1")
    print(resp)
    tableService.deleteTable(tableName)

def test_tabletCapacity():
    pass

def test_maxCellCopies():
    tableName = "testTable"
    table = createTable(tableName)
    tableService.createTable(table,2)
    tableService.addNewEntry(tableName,"first","cf1","c1","Hello!",123.0)
    tableService.addNewEntry(tableName,"first","cf1","c1","Hello!",124.0)
    tableService.addNewEntry(tableName,"first","cf1","c1","Hello!",125.0)
    cells = tableService.getEntry(tableName,"first","cf1","c1")
    if len(cells) != 2:
        raise "Error! Inexact number of entries"
    print(cells)
    for c in cells:
        if c[1] == 123.0:
            raise "Error: Newer value expected"
    tableService.deleteTable(tableName)

def test_memTableCapacity():
    tableName = "testTable"
    table = createTable(tableName)
    tableService.createTable(table,5,100,2)
    tableService.addNewEntry(tableName,"aaa","cf1","c1","Hello!",123.0)
    tableService.addNewEntry(tableName,"aab","cf1","c1","Hello!",124.0)
    tableService.addNewEntry(tableName,"ab","cf1","c1","Hello!",124.0)
    tableService.addNewEntry(tableName,"cd","cf1","c1","Hello!",124.0)
    tableService.addNewEntry(tableName,"dd","cf1","c1","Hello!",124.0)
    resp = tableService.getEntryRange(tableName,"aaa","d","cf1","c1")
    print(resp)
    if len(tableService.metaMgr.getRelevantTablet(tableName,"ab").ssTables) != 2:
        raise "Error: SSTable splits not working properly!"
    tableService.deleteTable(tableName)

def test_queryRecovery():
    tableName = "testTable"
    resp = tableService.getEntryRange(tableName,"aaa","d","cf1","c1")
    print(resp)
    if len(tableService.metaMgr.getRelevantTablet(tableName,"ab").ssTables) != 2:
        raise "Error: SSTable splits not working properly!"
    resp = tableService.getEntryRange(tableName,"aaa","d","cf1","c1")
    print(resp)
    tableService.deleteTable(tableName)


if __name__ == "__main__":
    init_service("metadata","sst","wal")
    test_createTable()
    test_deleteTable()
    test_listTables()
    test_getTableInfo()
    test_insertEntry()
    test_getRowRange()
    test_maxCellCopies()
    test_memTableCapacity()
    test_queryRecovery()