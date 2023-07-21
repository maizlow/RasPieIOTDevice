import database, aws_mqtt, variables
from modules.logging import Logging
import time, asyncio, threading, json
from awscrt import mqtt


TOPIC_PLC_INFO = "iot/device/" + variables.CLIENT_ID + "/plc_info"
TOPIC_TAGS = "iot/device/" + variables.CLIENT_ID + "/tags"
TOPIC_JOBS = "iot/device/" + variables.CLIENT_ID + "/jobs"

db = database.DB()
logging_active = False
plc_update = False
tag_update = False
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
    global plc_update
    if logging_active:
        plc_update = True
    if "delete" in topic:
        if db.deletePLC(payload):
            print("Successfully removed PLC with IP-address: ", payload["ip_address"])
        else:
            print("Failed to remove PLC with IP-address: '{}' No PLC found with this ip-address!".format(payload["ip_address"]))
    else:
        if db.insertPLC(payload):
            print("Successfully inserted/updated PLC info")
        else:
            print("Failed to insert/update PLC info")

def new_tag_message(topic, payload):
    global tag_update
    if logging_active:
        tag_update = True
    if "list" in topic:
        if db.dropAndInsertTags(payload):
            print("Successfully inserted tag list")
        else:
            print("Failed to insert tag list!")
    


async def main():
    global logging_active
    global plc_update
    global tag_update
    plc_list = []
    #Establish MQTT connection to AWS
    con = await aws_mqtt.connect_mqtt()
    
    #Subscribe to PLC_INFO, this will insert to DB if not existing or update existing ones
    await aws_mqtt.subscribe_to_topic(con, TOPIC_PLC_INFO + "/#", on_message_received)
    #Subscribe to TAGS, this will insert to DB if not existing or update existing ones
    await aws_mqtt.subscribe_to_topic(con, TOPIC_TAGS + "/#", on_message_received)
    
    while(True): 
        try:    
            plc_list = db.getAllActivePLC()   
                
            if plc_list.__len__() > 0:
                #When tag list and plc list have data, try to start logging
                threads = []        
                logging = []
                for plc in plc_list:      
                    tagList = db.getTagsForPLC(plc["ip_address"])
                    if len(tagList) > 0:
                        log = Logging(tagList, plc["ip_address"], db)
                        logging.append(log)
                        threads.append(threading.Thread(target=log.Logging, daemon=True))
                        threads[-1].start()

            #When we have started at least one thread don't recreate them...                
            while(threads.__len__() > 0):
                logging_active = True
                if tag_update or plc_update:
                    print("Changes to the taglist have been recieved!")
                    plc_list = db.getAllActivePLC() 
                    for plc in plc_list:                              
                        tagList = db.getTagsForPLC(plc["ip_address"])
                        if len(tagList) > 0:
                            for log in logging:                          
                                print(plc["ip_address"] + " ; " + log.plc)
                                if(log.plc == plc["ip_address"]):
                                    log.UpdateTags(tagList)
                                # else:
                                #     print("New thread started!")
                                #     log = Logging(tagList, plc["ip_address"], db)
                                #     logging.append(log)
                                #     threads.append(threading.Thread(target=log.Logging, daemon=True))
                                #     threads[-1].start()
                                #     break
                    tag_update = False
                    plc_update = False
                time.sleep(1)


            #Main thread interval
            time.sleep(10)

        except KeyboardInterrupt:
            for log in logging:
                log.Stop()
            for thread in threads:
                thread.join()
            await aws_mqtt.disconnect_mqtt(con)
            print("Exiting...")
            exit()


    
asyncio.run(main())



