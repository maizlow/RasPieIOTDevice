import aws_mqtt
import config
from database.local_db import MongoDB
from modules.logging import Logging
import time, asyncio, threading, json
from awscrt import mqtt
from bson.json_util import dumps, loads


CLIENT_ID = config.getClientID()

TOPIC_PLC_INFO = "iot/device/" + CLIENT_ID + "/plc_info"
TOPIC_TAGS = "iot/device/" + CLIENT_ID + "/tags"
TOPIC_JOBS = "iot/device/" + CLIENT_ID + "/jobs"
TOPIC_DATA = "iot/device/" + CLIENT_ID + "/data"


db = MongoDB()
logging_active = False
plc_update = False
tag_update = False
#Steg 1: Anslut till AWS, subscribe till PLC Info och Tagglista
#Steg 2: Kolla lokala databasen om info finns ang PLC och Taggar att logga
#Steg 3: Börja logga och publisha till AWS om anslutning är uppe annars logga till lokala DB

# Callback when the subscribed topic receives a message
def on_message_received(topic, payload, dup, qos, retain, **kwargs):
    #print("Received message from topic '{}': {}".format(topic, json.loads(payload)))
    print("Received message from topic '{}': {}".format(topic, payload))
    json_payload = payload #json.loads(payload)
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
    con = await aws_mqtt.connect_mqtt(config.getAWSEndpoint(), CLIENT_ID)
    
    #Subscribe to PLC_INFO, this will insert to DB if not existing or update existing ones
    await aws_mqtt.subscribe_to_topic(con, TOPIC_PLC_INFO + "/#", on_message_received)
    #Subscribe to TAGS, this will insert to DB if not existing or update existing ones
    await aws_mqtt.subscribe_to_topic(con, TOPIC_TAGS + "/#", on_message_received)
    
    while(True): 
        # ids = []
        # ids.append(1)
        # db.deleteDataPoints(ids)
        try:    
            plc_list = db.getAllActivePLC()
            threads = []  
            logging = [] 
            print(len(list(plc_list.clone())))  
            if len(list(plc_list.clone())) > 0:
                #When tag list and plc list have data, try to start logging       
                for plc in plc_list:      
                    tagList = db.getTagsForPLC(plc["ip_address"])
                    if len(list(tagList.clone())) > 0:
                        log = Logging(tagList, plc["ip_address"], db)
                        logging.append(log)
                        threads.append(threading.Thread(target=log.Logging, daemon=True))
                        threads[-1].start()

            #When we have started at least one thread don't recreate them...                
            while(threads.__len__() > 0):
                print(f"Number of active threads: {len(threads)}")
                logging_active = True
                if tag_update or plc_update:
                    print("Changes to the taglist have been recieved!")
                    plc_list = db.getAllActivePLC() 
                    for plc in plc_list:                              
                        tagList = db.getTagsForPLC(plc["ip_address"])
                        if len(list(tagList.clone())) > 0:
                            #Update existing logs
                            logged = False
                            for log in logging:                                                          
                                if(log.plc == plc["ip_address"]):                                    
                                    log.UpdateTags(tagList)
                                    logged = True
                                
                            #Start logging if new tags or plc found
                            if not logged:
                                print(f"New thread started for plc with IP: {plc['ip_address']}!")
                                newlog = Logging(tagList, plc["ip_address"], db)
                                logging.append(newlog)
                                threads.append(threading.Thread(target=logging[-1].Logging, daemon=True))
                                threads[-1].start()
                            
                    tag_update = False
                    plc_update = False

                #Check if tags are logged and publish them to AWS
                if aws_mqtt.isConnected:
                    loggedTags = db.getUnpublishedDatapoints()
                    publishedIds = []
                    for unpublished in loggedTags:
                        payload = {
                            "tag_id": dumps(unpublished["tag_id"]),
                            "value": unpublished["value"],
                            "timestamp": unpublished["timestamp"]
                        }
                        #await aws_mqtt.publish_to_topic(con, TOPIC_DATA, dumps(payload), mqtt.QoS.AT_LEAST_ONCE)
                        publishedIds.append(unpublished["_id"])

                    db.deleteDataPoints(publishedIds)


                time.sleep(1)


            #Main thread interval
            time.sleep(10)

        except KeyboardInterrupt:
            if logging:
                for log in logging:
                    log.Stop()
            if threads:    
                for thread in threads:
                    thread.join()
            await aws_mqtt.disconnect_mqtt(con)
            print("Exiting...")
            exit()


    
asyncio.run(main())



