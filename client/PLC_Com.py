from database.models.Plc import Plc
import snap7
import time, math

class PLC_Com():
    client = snap7.client.Client()

    def __init__(self, plc_info: Plc):
        self.plc_info = plc_info
        self.isConnected = False

    def connect(self):
        print(f"Connecting to PLC with IP: {self.plc_info.ip} -> Rack: {self.plc_info.rack} -> Slot: {self.plc_info.slot}")
        try:
            self.client.connect(
                self.plc_info.ip, self.plc_info.rack, self.plc_info.slot)
        except:
            print("Failed to connect to PLC! Retrying...")
        
        timeout = time.time() + 3 #3 second timeout
        retries = 1
        while not self.checkConnection():
            if time.time() > timeout:
                try:
                    self.client.connect(
                        self.plc_info.ip, self.plc_info.rack, self.plc_info.slot)
                except:
                    print("Failed to connect to PLC! Retrying...")
                    #Add another 3 seconds
                    timeout = time.time() + 3
            print(f"Retry attempt nr {retries} in {math.floor(timeout - time.time())}s")
            time.sleep(1)
            retries += 1
            
        if self.checkConnection():
            print(f"Connected to plc with IP: {self.plc_info.ip}")
            self.isConnected = True
        

    def checkConnection(self):
        return str(self.client.get_cpu_state()) == "S7CpuStatusRun"

    def read_db_dint(self, dbNumber, byteOffset):
        if self.checkConnection():
            read = self.client.db_read(dbNumber, byteOffset, 4)
            if read:               
                return snap7.util.get_dint(read, 0)
            else:
                return -1
