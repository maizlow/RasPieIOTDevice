from tinydb import TinyDB
from tinydb import Query
from models.plc import PLC_Model


db_name = "iot_db.json"
plc_table_name = "plc_table"
tags_table_name = "tags_table"
data_table_name = "data_table"

class DB:
    _db_plc = type(TinyDB)
    _db_tags = type(TinyDB)
    _db_data = type(TinyDB)

    def __init__(self) -> None:
        self._db_plc = TinyDB(db_name).table(plc_table_name)
        self._db_tags = TinyDB(db_name).table(tags_table_name)
        self._db_data = TinyDB(db_name).table(data_table_name)


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
    
    #Have to be all tags at once
    def insertTags(self, items):
        if items:
            tag = Query()
            #Delete all tags then insert new tags
            self._db_tags.remove(tag.id > 0)
            if self._db_tags.count(tag.id > 0):
                self._db_tags.insert_multiple(items)
                if self._db_tags.count(tag.id > 0) == 0:
                    print("Failed to insert tags!")
                else:
                    return True
            else:
                print("Failed to delete tags before insertions!")
                return False
        else:
            print("No tags to function!")
        return False

            
#endregion    
#region DATA

#endregion