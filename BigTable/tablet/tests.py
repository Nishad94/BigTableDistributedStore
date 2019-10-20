from TableService import TableService
import Table

tableService = None 

def init_service(metadataPath, ssTablePath, walPath):
    global tableService
    tableService = TableService(metadataPath, ssTablePath, walPath)

def test_createTable():
    tableName = "testTable"
    colFams = ["cf1","cf2"]
    cols = [["c1","c2"],["c1","c2"]]
    colFam1 = Table.ColumnFamily(colFams[0],cols[0])
    colFam2 = Table.ColumnFamily(colFams[1],cols[1])
    table = Table.Table(tableName,[colFam1,colFam2])

    tableService.createTable(table)
    tables = tableService.listTables()
    if tables[0] != tableName:
        raise Exception("Error in creating table: Table not found")
    tableService.deleteTable(tableName)

def test_deleteTable():
    tableName = "testTable"
    colFams = ["cf1","cf2"]
    cols = [["c1","c2"],["c1","c2"]]
    colFam1 = Table.ColumnFamily(colFams[0],cols[0])
    colFam2 = Table.ColumnFamily(colFams[1],cols[1])
    table = Table.Table(tableName,[colFam1,colFam2])
    tableService.createTable(table)

    tableService.deleteTable(tableName)
    # TODO: check if sst + wal files deleted
    tables = tableService.listTables()
    if tableName in tables:
        raise Exception("Error: Found deleted tables in metadata manager!")

if __name__ == "__main__":
    init_service("metadata","sst","wal")
    test_createTable()
    test_deleteTable()