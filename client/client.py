import database, aws_mqtt, variables
from models import plc
import time, asyncio, threading, json
from awscrt import mqtt


TOPIC_PLC_INFO = "iot/device/" + variables.CLIENT_ID + "/plc_info"
TOPIC_TAGS = "iot/device/" + variables.CLIENT_ID + "/tags"
TOPIC_JOBS = "iot/device/" + variables.CLIENT_ID + "/jobs"

db = database.DB()

#Steg 1: Anslut till AWS, subscribe till PLC Info och Tagglista
#Steg 2: Kolla lokala databasen om info finns ang PLC och Taggar att logga
#Steg 3: Börja logga och publisha till AWS om anslutning är uppe annars logga till lokala DB

# Callback when the subscribed topic receives a message
def on_message_received(topic, payload, dup, qos, retain, **kwargs):
    print("Received message from topic '{}': {}".format(topic, json.loads(payload)))
    json_payload = json.loads(payload)
    if TOPIC_PLC_INFO in topic:
        new_plc_message(topic, json_payload)
    elif TOPIC_TAGS in topic:
        new_tag_message(topic, json_payload)
    elif TOPIC_JOBS in topic:
        pass

def new_plc_message(topic, payload):
    if "delete" in topic:
        if database.DB.deletePLC(db, payload):
            print("Successfully removed PLC with IP-address: ", payload["ip_address"])
        else:
            print("Failed to remove PLC with IP-address: '{}' No PLC found with this ip-address!".format(payload["ip_address"]))
    else:
        if database.DB.insertPLC(db, payload):
            print("Successfully inserted/updated PLC info")
        else:
            print("Failed to insert/update PLC info")

def new_tag_message(topic, payload):
    pass


async def main():
    #Establish MQTT connection to AWS
    con = await aws_mqtt.connect_mqtt()
    
    #Subscribe to PLC_INFO, this will insert to DB if not existing or update existing ones
    await aws_mqtt.subscribe_to_topic(con, TOPIC_PLC_INFO + "/#", on_message_received)
    #Subscribe to TAGS, this will insert to DB if not existing or update existing ones
    await aws_mqtt.subscribe_to_topic(con, TOPIC_TAGS + "/#", on_message_received)
    
    quit = False
    while(not quit):





        time.sleep(10)


    await aws_mqtt.disconnect_mqtt(con)

asyncio.run(main())





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