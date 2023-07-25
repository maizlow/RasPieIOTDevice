from database.models.Plc import Plc
import snap7
import time

class PLC_Com():
    client = snap7.client.Client()

    def __init__(self, plc_info: Plc):
        self.plc_info = plc_info

    def connect(self):
        print(f"Connecting to PLC with IP: {self.plc_info.ip} -> Rack: {self.plc_info.rack} -> Slot: {self.plc_info.slot}")
        err = self.client.connect(
            self.plc_info.ip, self.plc_info.rack, self.plc_info.slot)
        if err:
            print(f"Failed to connect, error code: {str(err)}")
            exit()
        
        timed_out = False
        timeout = time.time() + 3 #3 second timeout
        while not self.client.get_connected() or timed_out:
            timed_out = time.time() > timeout
            
        if self.client.get_connected():
            print(f"Connected to plc with IP: {self.plc_info.ip}")
        else:
            print("Connection timed out and failed!")
            self.client.destroy()

    def read_db_dint(self, dbNumber, byteOffset):
        if self.client.get_connected():
            read = self.client.db_read(dbNumber, byteOffset, 4)
            if read:               
                return snap7.util.get_dint(read, 0)
            else:
                return -1
