from database.models.Plc import Plc
from database.models.tag import DataType
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
                    retries += 1
                    self.client.connect(
                        self.plc_info.ip, self.plc_info.rack, self.plc_info.slot)
                except:
                    print("Failed to connect to PLC! Retrying...")
                    #Add another 3 seconds
                    timeout = time.time() + 3
            print(f"Retry attempt nr {retries} in {math.floor(timeout - time.time())}s")
            time.sleep(1)
            
        if self.checkConnection():
            print(f"Connected to plc with IP: {self.plc_info.ip}")
            self.isConnected = True
        

    def checkConnection(self):
        return str(self.client.get_cpu_state()) == "S7CpuStatusRun"

    def read_db_value(self, dbNumber : int, startByte : int, byteLength : int, dataType : DataType):
        """
        Reads any value from a DB in the connected PLC. For boolean values use read_db_bit.    
        """
        if dataType == DataType.Bit or dataType == DataType.String:
            return None
        if self.checkConnection():
            print(f"Size: {byteLength}")
            read = self.client.db_read(dbNumber, startByte, byteLength)
            if read:              
                if dataType == DataType.Byte: 
                    return snap7.util.get_byte(read, 0)
                elif dataType == DataType.Char: 
                    return snap7.util.get_char(read, 0)
                elif dataType == DataType.Word: 
                    return snap7.util.get_word(read, 0)
                elif dataType == DataType.Int: 
                    return snap7.util.get_int(read, 0)
                elif dataType == DataType.DWord: 
                    return snap7.util.get_dword(read, 0)
                elif dataType == DataType.DInt: 
                    return snap7.util.get_dint(read, 0)
                elif dataType == DataType.Real: 
                    return snap7.util.get_real(read, 0)
            else:
                return None

    def read_db_bit(self, dbNumber, startByte, bit):
        if self.checkConnection():
            read = self.client.db_read(dbNumber, startByte, 1)
            if read:              
                return snap7.util.get_bool(read, 0, bit)
            else:
                return None
            
    def read_db_string(self, dbNumber, startByte):
        if self.checkConnection():
            read = self.client.db_read(dbNumber, startByte, 254)
            if read:              
                return snap7.util.get_string(read, 0)
            else:
                return None
