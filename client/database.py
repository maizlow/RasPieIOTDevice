from tinydb import TinyDB
from tinydb import Query
from models.plc_model import PLC_Model


db_name = "iot_db.json"
plc_table_name = "plc_table"
data_table_name = "data_table"

class DB:
    _db_plc = type(TinyDB)
    _db_data = type(TinyDB)

    def __init__(self) -> None:
        self._db_plc = TinyDB(db_name).table(plc_table_name)
        self._db_data = TinyDB(db_name).table(data_table_name)


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
                return True
         return False    