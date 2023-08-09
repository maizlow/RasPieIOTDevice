import aws_mqtt, glob, csv
import config
from database.local_db import MongoDB
from modules.logging import Logging
import time, asyncio, threading, json
from awscrt import mqtt
from bson.json_util import dumps, loads
from database.models.Plc import Plc
import variables

THING_NAME = config.getThingName()
CLIENT_ID = config.getClientID()

TOPIC_PLC_INFO = "iot/device/" + THING_NAME + "/plc_info"
TOPIC_TAGS = "iot/device/" + THING_NAME + "/tags"
TOPIC_DATATYPES = "iot/device/" + THING_NAME + "/datatypes"
TOPIC_JOBS = "iot/device/" + THING_NAME + "/jobs"
TOPIC_DATA = "iot/device/" + THING_NAME + "/data"


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
    #print("Received message from topic '{}': {}".format(topic, payload))
    json_payload = json.loads(payload)
    if TOPIC_PLC_INFO in topic:
        new_plc_message(topic, json_payload)
    elif TOPIC_TAGS in topic:
        new_tag_message(topic, json_payload)
    elif TOPIC_DATATYPES in topic:
        new_datatypes_message(topic, json_payload)
    elif TOPIC_JOBS in topic:
        pass

def new_plc_message(topic, payload):
    global plc_update
    if logging_active:
        plc_update = True
    if "delete" in topic:
        if db.deletePLC(payload["ip"]):
            print("Successfully removed PLC with IP-address: ", payload["ip"])
        else:
            print("Failed to remove PLC with IP-address: '{}' No PLC found with this ip-address!".format(payload["ip"]))
    else:
        if db.UpsertPLCs(payload):#db.InsertPLCs(payload):
            print("Successfully inserted/updated PLC info")
        else:
            print("Failed to insert/update PLC info")

def new_tag_message(topic, payload):
    global tag_update
    if logging_active:
        tag_update = True
    if "list" in topic:
        if db.UpsertTags(payload):
            print("Successfully inserted / updated tag list")
        else:
            print("Failed to insert tag list!")
    
def new_datatypes_message(topic, payload):
    if db.dropAndInsertDatatypes(payload):
        print("Successfully inserted new datatypes")
    else:
        print("Faild to insert new datatypes")

async def main():
    """
    data_dict = {}
    print(variables.ALARM_LOG_CSV_FILEPATH)
    for fpath in glob.glob(variables.ALARM_LOG_CSV_FILEPATH + "/*.csv"):
        print(fpath)
        try:
            with open(fpath) as csv_file_handler:
                csv_reader = csv.DictReader(csv_file_handler)
                i = 1
                for rows in csv_reader:
                    key = i
                    data_dict[key] = rows
                    i += 1

            with open(variables.ALARM_LOG_CSV_FILEPATH + "/logAsJson.json", 'w', encoding = 'utf-8') as json_file_handler:                
                json_file_handler.write(json.dumps(data_dict, indent = 4))

            
        except Exception as e:
            print(f"Failed with: {fpath}")
            print(e)

    
    return
    """
    global logging_active
    global plc_update
    global tag_update
    plc_list = []
    #Establish MQTT connection to AWS
    con = await aws_mqtt.connect_mqtt(config.getAWSEndpoint(), THING_NAME)
    
    #Subscribe to Datatypes, this will insert to DB if not existing or update existing ones
    await aws_mqtt.subscribe_to_topic(con, TOPIC_DATATYPES, on_message_received)
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
            if len(list(plc_list.clone())) > 0 and db.CountDatatypes() >= 9:
                #When tag list and plc list have data, try to start logging       
                for plc in plc_list:      
                    tagList = db.getTagsForPLC2(plc["id"])
                    
                    if len(tagList) > 0:
                        print("Taglist length is:", len(tagList))
                        logging.append(Logging(tagList, plc, db))
                        threads.append(threading.Thread(target=logging[-1].Logging, daemon=True))
                        threads[-1].start()

            #When we have started at least one thread don't recreate them...                
            while(threads.__len__() > 0):
                print(f"Number of active threads: {len(threads)}")
                logging_active = True
                if tag_update or plc_update:
                    print("Changes to the taglist have been recieved!")
                    plc_list = db.getAllActivePLC() 
                    for plc in plc_list:                              
                        tagList = db.getTagsForPLC2(plc["id"])
                        if len(tagList) > 0:
                            #Update existing logs
                            logged = False
                            for log in logging:                                                          
                                if(log.plc.ip == plc["id"]):                                    
                                    log.UpdateTags(tagList)
                                    logged = True
                                
                            #Start logging if new tags or plc found
                            if not logged:
                                print(f"New thread started for plc with IP: {plc['ip']}!")
                                logging.append(Logging(tagList, plc, db))
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
                            "value_dt": str.split(str(type(unpublished["value"])), "'")[1],
                            "value": str(unpublished["value"]),
                            "timestamp": unpublished["timestamp"]
                        }                                              
                        
                        if config.getPublishStatus():
                            await aws_mqtt.publish_to_topic(con, TOPIC_DATA, dumps(payload), mqtt.QoS.AT_LEAST_ONCE)
                        #Test
                        publishedIds.append(unpublished["_id"])
                    
                    db.deleteDataPoints(publishedIds)


                time.sleep(1)


            #Main thread interval
            time.sleep(10)

        except KeyboardInterrupt:
            if logging:
                for log in logging:
                    #print(log.plc.ip)
                    log.Stop()
            if threads:    
                for thread in threads:
                    thread.join()
            await aws_mqtt.disconnect_mqtt(con)
            print("Exiting...")
            exit()


    
asyncio.run(main())



