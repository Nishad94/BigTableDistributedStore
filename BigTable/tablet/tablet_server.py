import json
import sys
import os
from flask import Flask, request
from flask import Response
from TableService import TableService
from Table import Table, ColumnFamily

app = Flask(__name__)

tableService = None

@app.route('/api/tables/', methods=['GET'])
def list_tables():
    tables = tableService.listTables()
    resp = {}
    resp["tables"] = tables
    resp = Response(json.dumps(resp), status=200, content_type='application/json')
    return resp

@app.route('/api/tables/', methods=['POST'])
def create_table():
    try:
        req = json.loads(request.data)
    except:
        return Response(None,400)

    tableName = req["name"]
    colFams = req["column_families"]

    colFamObj = []
    
    for cf in colFams:
        cf_name = cf["column_family_key"]
        cols = []
        for c in cf["columns"]:
            cols.append(c)
        colFamObj.append(ColumnFamily(cf_name,cols))
    
    table = Table(tableName,colFamObj)
    created = tableService.createTable(table)
    if created is True:
        resp = Response(None, status=200)
    else:
        resp = Response(None, status=409)
    return resp

@app.route('/api/tables/<pk>', methods=['DELETE'])
def destroy_table(pk):
    if tableService.tableExists(pk) is False:
        return Response(None,404)
    tableService.deleteTable(pk)
    return Response(None,200)

@app.route('/api/tables/<pk>', methods=['GET'])
def get_table_info(pk):
    if tableService.tableExists(pk) is False:
        return Response(None,404)
    info = tableService.getTableInfo(pk)
    return Response(info.getAsJson(),200,content_type="application/json")

@app.route('/api/table/<pk>/cell', methods=['POST'])
def insert_cell(pk):
    try:
        req = json.loads(request.data)
    except:
        return Response(None,404)
    tableName = pk
    rowKey = req["row"]
    cf = req["column_family"]
    col = req["column"]
    data_val = [r["value"] for r in req["data"]]
    data_time = [float(r["time"]) for r in req["data"]]
    if tableService.tableExists(pk) is False:
        return Response(None,404)
    t_info = tableService.getTableInfo(pk)
    t_cf = None
    for cFam in t_info.columnFamilies:
        if cFam.name == cf:
            t_cf = cFam
            break
    if t_cf is None or col not in t_cf.columns:
        return Response(None,400)
    for dv,dt in zip(data_val,data_time):
        tableService.addNewEntry(tableName,rowKey,cf,col,dv,dt)
    return Response(None,200)   

@app.route('/api/table/<pk>/cell', methods=['GET'])
def retrieve_cell(pk):
    try:
        req = json.loads(request.data)
    except:
        return Response(None,404)
    tableName = pk
    rowKey = req["row"]
    cf = req["column_family"]
    col = req["column"]
    if tableService.tableExists(pk) is False:
        return Response(None,404)
    t_info = tableService.getTableInfo(pk)
    t_cf = None
    for cFam in t_info.columnFamilies:
        if cFam.name == cf:
            t_cf = cFam
            break
    if t_cf is None or col not in t_cf.columns:
        return Response(None,400)
    content = tableService.getEntry(tableName,rowKey,cf,col)
    resp = {}
    resp["row"] = rowKey
    resp["data"] = []
    if content is not None:
        for c in content:
            resp["data"].append({"value":c[0],"time":c[1]})
    return Response(json.dumps(resp),200)


@app.route('/api/table/<pk>/cells', methods=['GET'])
def retrieve_cells(pk):
    req = json.loads(request.data)
    tableName = pk
    cf = req["column_family"]
    col = req["column"]
    if tableService.tableExists(pk) is False:
        return Response(None,404)
    t_info = tableService.getTableInfo(pk)
    t_cf = None
    for cFam in t_info.columnFamilies:
        if cFam.name == cf:
            t_cf = cFam
            break
    if t_cf is None or col not in t_cf.columns:
        return Response(None,400)
    rowStart = str(req["row_from"])
    rowEnd = str(req["row_to"])
    rows = tableService.getEntryRange(tableName,rowStart,rowEnd,cf,col)
    resp = {}
    resp["rows"] = []
    if rows is not None:
        for rowName,vals in rows.items():
            inner = {}
            inner["row"] = rowName
            inner["data"] = []
            for v in vals:
                inner["data"].append({"value":v[0],"time":v[1]})
            resp["rows"].append(inner)
    return Response(json.dumps(resp),200)


@app.route('/api/table/<pk>/row', methods=['GET'])
def retrieve_row(pk):
    req = json.loads(request.data)
    tableName = pk
    if tableService.tableExists(pk) is False:
        return Response(None,404)
    entry = tableService.getEntry(tableName,req["row"])
    resp = {}
    resp["row"] = req["row"]
    resp["column_families"] = []
    if entry is not None:
        for cfName,colDict in entry.items():
            x = {}
            x[cfName] = {}
            x[cfName]["columns"] = []
            for colName,content in colDict.items():
                y = {}
                y[colName] = {}
                y[colName]["data"] = []
                for data in content:
                    y[colName]["data"].append({"value":data[0],"time":data[1]})
                x[cfName]["columns"].append(y)
            resp["column_families"].append(x)
    return Response(json.dumps(resp),200)


@app.route('/api/memtable', methods=['POST'])
def set_memtable_max():
    try:
        newVal = json.loads(request.data)
    except:
        return Response(None,400)
    tables = tableService.listTables()
    for t in tables:
        tableService.changeMemtableCapacity(t,int(newVal["memtable_max"]))
    return Response(None,200)


if __name__ == '__main__':
    walPath = sys.argv[5]
    ssTablePath = sys.argv[6]
    metadataPath = "metadata"
    ## These meta files must be removed for clean slate!
    if os.path.exists(metadataPath) is False:
        os.mkdir(metadataPath)
    if os.path.exists(ssTablePath) is False:
        os.mkdir(ssTablePath)
    if os.path.exists(walPath) is False:
        os.mkdir(walPath)
    tableService = TableService(metadataPath, ssTablePath, walPath)
    app.run(host=sys.argv[1], port=sys.argv[2])
    