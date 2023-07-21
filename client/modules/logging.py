from time import time
from classes.tag import Tag

class Logging(object):

    def __init__(self, tags, plc, db):
        #PLC info finns på varje tagg
        self.tags = []
        self.prepareTaglist(tags) 
        self.plc = plc
        self.db = db
        self.is_running = True
        self.lock_request = False
        self.lock_active = False
        
    
    def Stop(self):
        self.is_running = False
        print(f"Logging class for: {self.plc} - Stop command!")

    def Lock(self):
        self.lock_request = True
        print(f"Logging class for: {self.plc} - Lock request!")

    def Unlock(self):
        self.lock_active = False
        self.lock_request = False
        print(f"Logging class for: {self.plc} - Unlock request!")

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
            newTag = Tag(str(tag["id"]), tag["db-nr"], tag["start-address"], tag["data-type"], tag["log-interval"], tag["batch-interval"], tag["PLC_IP"])            
            self.tags.append(newTag)

    def Logging(self):
        while self.is_running:
            for tag in self.tags:
                tag : Tag
                now = time()
                if now >= tag.next_logging and tag.next_logging > 0:
                    tag.next_logging = now + tag.log_interval
                    #Read tag with snap7 and log to database
                    print(f"Logging tag, DB={tag.db_nr}, Start address={tag.start_address}, Data type={tag.data_type} from PLC with IP: {tag.PLC_IP}")
                    #t = LoggedTag(x._id, 512, x.PLC_Id)
                    #print(t.toJSON())
                    #self.mqttClient.publish(x._id, 1, x.description)
        
                else:
                    if tag.next_logging == 0:
                        tag.next_logging = now + tag.log_interval

            # print(str(len(self.tags)))
            #sleep(0.5) #Bör göras om så man inte stannar loopningen
            #print(f"Time: {time()}")
            
            #While updating the tag list stop the looping above until done
            while self.lock_request:
                self.lock_active = True