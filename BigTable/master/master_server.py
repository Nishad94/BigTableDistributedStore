import json
import sys
import os
import requests
from flask import Flask, request
from flask import Response
from MasterService import MasterService
from TableService import TableService
from Table import Table, ColumnFamily

app = Flask(__name__)

masterService = None

@app.route('/api/register/', methods=['POST'])
def register_tabletServer():
    try:
        req = json.loads(request.data)
    except:
        return Response(None,400)
    server_host = req["host"]
    server_port = req["port"]
    masterService.register_tablet_server(server_host+":"+server_port)
    return Response(None,200)

@app.route('/api/tables/', methods=['GET'])
def list_tables():
    tables = masterService.list_tables()
    resp = {}
    resp["tables"] = tables
    resp = Response(json.dumps(resp), status=200, content_type='application/json')
    return resp

@app.route('/api/tables/', methods=['POST'])
def create_table():
    curr_server_id = masterService.get_server_ptr()
    return requests.post(f"http://{curr_server_id}/api/tables/",data=request.data)

@app.route('/api/tables/<pk>', methods=['DELETE'])
def destroy_table(pk):
    tableName = pk
    if masterService.is_locked(tableName) is True:
        return Response(None,409)
    servers = masterService.get_table_servers(tableName)
    try:
        for serv in servers:
            requests.delete(f"http://{serv}/api/tables/{tableName}")
            masterService.remove_table(tableName)
    except:
        return Response(None,404)
    return Response(None,200)

@app.route('/api/tables/<pk>', methods=['GET'])
def get_table_info(pk):
    tableName = pk
    if masterService.table_exists(tableName) is False:
        return Response(None,404)
    info = masterService.get_table_info(pk)
    return Response(json.dumps(info),200,content_type="application/json")


if __name__ == '__main__':
    masterService = MasterService()
    app.run(host=sys.argv[1], port=sys.argv[2])
    