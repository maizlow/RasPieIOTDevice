import datetime, random
import threading
from datetime import timezone
from database.models.tag import Tag, DataType
from database.local_db import MongoDB
from PLC_Com import PLC_Com
from database.models.Plc import Plc

class Logging(object):

    def __init__(self, tags, plc : Plc, db):
        #PLC info finns på varje tagg
        self.tags = []
        self.prepareTaglist(tags) 
        self.plc = Plc(plc["_id"], plc["ip_address"], plc["rack"], plc["slot"], plc["active"])
        print(plc["_id"])
        self.db : MongoDB = db
        self.is_running = True
        self.lock_request = False
        self.lock_active = False
        self.plc_com = PLC_Com(self.plc)
               
    
    def Stop(self):
        self.is_running = False
        print(f"Logging class for: {self.plc.ip} - Stop command!")

    def Lock(self):
        self.lock_request = True
        print(f"Logging class for: {self.plc.ip} - Lock request!")

    def Unlock(self):
        self.lock_active = False
        self.lock_request = False
        print(f"Logging class for: {self.plc.ip} - Unlock request!")

    #Public function to update tag list in thread safe manner
    def UpdateTags(self, tags):
        self.Lock()
        while not self.lock_active:
            pass
        self.prepareTaglist(tags)
        self.Unlock()

    def prepareTaglist(self, taglist):
        self.tags.clear()
        for tag in taglist:
            tag : Tag
            #print(tag["PLC"][0])
            newTag : Tag
            #Exempel på hur man kommer in i de aggregerade datan
            #print(tag['PLC'][0]['IP-address'])
            newTag = Tag(str(tag["_id"]), tag["db_nr"], tag["start-address"], tag["data-type"], tag["bit_nr"], tag["log-interval"], tag["batch-interval"], tag["PLC_IP"])            
            self.tags.append(newTag)
        print("Tag list is prepared for logging!")

    def Logging(self):
        while self.is_running:      
            if not self.plc_com.checkConnection():
                self.plc_com.connect()
            for tag in self.tags:
                tag : Tag
                now = datetime.datetime.now(timezone.utc).timestamp()
                if now >= tag.next_logging and tag.next_logging > 0:
                    tag.next_logging = now + tag.log_interval
                    
                    print(f"Logging tag, DB={tag.db_nr}, Start address={tag.start_address}, Data type={tag.data_type} from PLC with IP: {tag.PLC_IP}")
                    
                    if self.plc_com.checkConnection():
                        #Read tag with snap7 and log to database
                        if tag.data_type == DataType.Bit:
                            value = self.plc_com.read_db_bit(tag.db_nr, tag.start_address, tag.bit_nr)
                        elif tag.data_type == DataType.String:
                            value = self.plc_com.read_db_string(tag.db_nr, tag.start_address)
                        else:
                            value = self.plc_com.read_db_value(tag.db_nr, tag.start_address, tag.getByteLength(tag.data_type), tag.data_type)
                        
                        if value:
                            self.db.insertDataPoint(tag._id, value, now)
                        else:
                            print("ERROR: Payload not configured correctly, no variable found on PLC with the attributes!")
                    else:
                        print(f"Lost connection to PLC: {self.plc.ip}")
                else:
                    if tag.next_logging == 0:
                        tag.next_logging = now + tag.log_interval
                        
            #While updating the tag list stop the looping above until done
            while self.lock_request:
                self.lock_active = True

    def getByteLength(self, dataType : int) -> int:
        """Returns number of bytes to read depending on data type.
        """
        if dataType == int(DataType.Bit): 
            return 1
        elif dataType == int(DataType.Byte): 
            return 1
        elif dataType == int(DataType.Char): 
            return 1
        elif dataType == int(DataType.Word): 
            return 2
        elif dataType == int(DataType.Int): 
            return 2
        elif dataType == int(DataType.DWord): 
            return 4
        elif dataType == int(DataType.DInt): 
            return 4
        elif dataType == int(DataType.Real): 
            return 4
        elif dataType == int(DataType.String): 
            return 254