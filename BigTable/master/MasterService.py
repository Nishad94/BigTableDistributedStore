import requests

class MasterService:
    def __init__(self):
        """ Service for fetching tablet server host:names and table to tablet mapping info.
        This service is also responsible for sending periodic heartbeats to all tablet servers to check their health, 
        and update its internal data structures.

        tablet_servers ({"serverId":"health"}): health = OK/DOWN
        tableTabletIdx ({tname:list[tablets]})
        """
        self.tablet_servers = {}
        self.tableTabletIdx = {}
        self.table_locks = {}
        self.current_server_ptr = 0
    
    def remove_table(self, t_name):
        del self.tableTabletIdx[t_name]
        del self.table_locks[t_name]
    
    def get_server_ptr(self):
        return self.current_server_ptr
    
    def increment_server_ptr(self):
        self.current_server_ptr += 1
        self.current_server_ptr = self.current_server_ptr % len(self.tablet_servers)
    
    def lock_table(self, t_name):
        self.table_locks[t_name] = 1
    
    def open_table(self, t_name):
        self.table_locks[t_name] = 0
    
    def is_locked(self, t_name):
        return self.table_locks[t_name] == 1

    def get_table_servers(self, t_name):
        tablets = self.tableTabletIdx[t_name]
        servers = []
        for t in tablets:
            servers.append(t.serverId)
        return servers

    def register_tablet_server(self, ip_port):
        self.tablet_servers.append(ip_port)
    
    def get_table_info(self, t_name):
        resp = {}
        resp["name"] = t_name
        resp["tablets"] = []
        for tablet in self.tableTabletIdx[t_name]:
            tablet_info = {}
            hostname_port = tablet.serverId
            tablet_info["hostname"] = hostname_port.split(":")[0]
            tablet_info["port"] = hostname_port.split(":")[1]
            tablet_info["row_from"] = tablet.start_key
            tablet_info["row_to"] = tablet.end_key
            resp["tablets"].append(tablet_info)
        return resp
    
    def list_tables(self):
        return self.tableTabletIdx.keys()
    
    def heartbeat_server(self, host, port):
        table_names = self.tableTabletIdx.keys()
        self.tableTabletIdx = {}
        for server in self.tablet_servers:
            for t_name in table_names:
                try:
                    tablets = requests.get(f"http://{host}:{port}/api/heartbeat/{t_name}")
                except:
                if t_name in self.tableTabletIdx:
                    curr_tablets = self.tableTabletIdx[t_name]
                else:
                    curr_tablets = []
                curr_tablets.extend(tablets)
                self.tableTabletIdx[t_name] = curr_tablets
        
    def table_exists(self, t_name):
        return t_name in self.tableTabletIdx

