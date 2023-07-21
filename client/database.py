from tinydb import TinyDB
from tinydb import Query


db_name = "iot_db.json"
plc_table_name = "plc_table"
tags_table_name = "tags_table"
data_table_name = "data_table"

class DB:
    _db_plc = type(TinyDB)
    _db_tags = type(TinyDB)
    _db_data = type(TinyDB)

    def __init__(self) -> None:
        self.db = TinyDB(db_name)
        self._db_plc = self.db.table(plc_table_name)
        self._db_tags = self.db.table(tags_table_name)
        self._db_data = self.db.table(data_table_name)


#region PLC
    def getPLC(self, id):
        plc = Query()
        return self._db_plc.get(plc.id == id)

    def getAllPLC(self):
        return self._db_plc.all()

    def getAllActivePLC(self):
        plc = Query()
        return self._db_plc.search(plc.active == True)

    def insertPLC(self, item):
         if item:
              plc = Query()
              if self._db_plc.count(plc.ip_address == item["ip_address"]) < 1:
                self._db_plc.insert(item)
                print("Inserted new PLC: ", item)
                return True
              else:
                  self._db_plc.update(item, plc.ip_address == item["ip_address"])
                  print("Updated PLC with IP-address: ", item["ip_address"])
                  return True
         return False    
    
    def deletePLC(self, item):
        if item:
            plc = Query()
            if self._db_plc.remove(plc.ip_address==item["ip_address"]):                
                return True
        return False
#endregion    
#region TAGS
    def getAllTags(self):
        return self._db_tags.all()
    
    def getTagsForPLC(self, PLC_IP):
        q = Query()
        return self._db_tags.search(q.PLC_IP == PLC_IP)

    #Have to be all tags at once
    def dropAndInsertTags(self, items):
        if items:
            tag = Query()
            #Delete all tags then insert new tags            
            self.db.drop_table(tags_table_name)
            if tags_table_name in self.db.tables():
                 print("Failed to drop tag table before insertion!")
                 return False
            
            if len(self._db_tags.insert_multiple(items)) > 0:
                print("Success!")
                return True
            else:
                print(self.db.tables())


            
#endregion    
#region DATA

#endregion