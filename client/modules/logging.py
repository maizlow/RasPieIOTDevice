import datetime, random
from datetime import timezone
from database.models.tag import Tag
from database.local_db import MongoDB
from PLC_Com import PLC_Com
from database.models.Plc import Plc

class Logging(object):

    def __init__(self, tags, plc : Plc, db):
        #PLC info finns på varje tagg
        self.tags = []
        self.prepareTaglist(tags) 
        self.plc = Plc(plc["_id"], plc["ip_address"], plc["rack"], plc["slot"], plc["active"])
        self.db : MongoDB = db
        self.is_running = True
        self.lock_request = False
        self.lock_active = False
        self.plc_com = PLC_Com(self.plc)
        self.plc_com.connect()        
    
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
            newTag = Tag(str(tag["_id"]), tag["db_nr"], tag["start-address"], tag["data-type"], tag["log-interval"], tag["batch-interval"], tag["PLC_IP"])            
            self.tags.append(newTag)
        print("Tag list is prepared for logging!")

    def Logging(self):
        print(f"{self.plc} Tag list length: {len(self.tags)}")
        while self.is_running:      
            for tag in self.tags:
                tag : Tag
                now = datetime.datetime.now(timezone.utc).timestamp()
                if now >= tag.next_logging and tag.next_logging > 0:
                    tag.next_logging = now + tag.log_interval
                    
                    print(f"Logging tag, DB={tag.db_nr}, Start address={tag.start_address}, Data type={tag.data_type} from PLC with IP: {tag.PLC_IP}")
                    
                    #Read tag with snap7 and log to database
                    value = self.plc_com.read_db_dint(tag.db_nr, tag.start_address)
                    if value:
                        self.db.insertDataPoint(tag._id, value, now)
                else:
                    if tag.next_logging == 0:
                        tag.next_logging = now + tag.log_interval
                        
            #While updating the tag list stop the looping above until done
            while self.lock_request:
                self.lock_active = True