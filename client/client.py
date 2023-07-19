import database
from models import plc_model

def main():
    myDB = database.DB()
    plc = plc_model.PLC_Model("192.168.11.2", 1, 2, True)
    if myDB.insertPLC(plc.getModelAsRecordForInsert()):
        print("Inserted record of PLC successfully!")
        for item in myDB.getAllActivePLC():
            print(item)
        
    else:
        print("Failed to insert PLC!")




main()