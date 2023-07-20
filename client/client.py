import database, aws_mqtt
from models import plc

#Steg 1: Anslut till AWS, subscribe till PLC Info och Tagglista
#Steg 2: Kolla lokala databasen om info finns ang PLC och Taggar att logga
#Steg 3: Börja logga och publisha till AWS om anslutning är uppe annars logga till lokala DB


def temp():
    m = aws_mqtt.MQTTClass()
    m.connect_mqtt()

temp()





"""

def main():
    myDB = database.DB()
    newplc = plc.PLC_Model("192.168.11.2", 1, 2, True)
    if myDB.insertPLC(newplc.getModelAsRecordForInsert()):
        print("Inserted record of PLC successfully!")
        for item in myDB.getAllActivePLC():
            print(item)
        
    else:
        print("Failed to insert PLC!")




main()
"""