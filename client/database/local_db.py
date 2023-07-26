from pymongo import MongoClient, errors
from bson import ObjectId
import config, platform

class MongoDB():
    def __init__(self) -> None:
        db_config = config.getConfiguredDatabase()
        usernamePassword = config.getMongoAuth()
        #Initiate client
        if platform.system() == "Linux":
            conStr = f"mongodb://{usernamePassword[0]}:{usernamePassword[1]}@{db_config.ip}:{db_config.port}"
        else:
            conStr = f"mongodb://{db_config.ip}:{db_config.port}"
        self.client = MongoClient(conStr)
        #Create DB if not exists
        self.db = self.client[db_config.name]
        #Check if collection exists otherwise create
        try:
            self.db.validate_collection("PLCs")     # Try to validate a collection
        except errors.OperationFailure:             # If the collection doesn't exist
            self.create_collections(0)
        try:
            self.db.validate_collection("Tags")      # Try to validate a collection
        except errors.OperationFailure:             # If the collection doesn't exist
            self.create_collections(1)
        try:
            self.db.validate_collection("Data")      # Try to validate a collection
        except errors.OperationFailure:             # If the collection doesn't exist
            self.create_collections(2)

    def create_collections(self, selection):
        if selection == 0:
            #Create collection for PLCs and add validation to schema
            self.db.drop_collection("PLCs")
            col = self.db.create_collection('PLCs', validator={
                '$jsonSchema': {
                    'bsonType': 'object',
                    'additionalProperties': False,
                    'required': ['ip_address', 'slot', 'rack', 'active'],
                    'properties': {
                        '_id': {
                            'bsonType': 'objectId'
                        },
                        'ip_address': {
                            'bsonType': 'string'
                        },
                        'slot': {
                            'bsonType': 'int'
                        },
                        'rack': {
                            'bsonType': 'int'
                        },
                        'active': {
                            'bsonType': 'bool'
                        }
                    }
                }
            })
            col.create_index('ip_address', unique=True)

        if selection == 1:
            #Create collection for Tags and add validation to schema
            self.db.drop_collection("Tags")
            self.db.create_collection('Tags', validator={
                '$jsonSchema': {
                    'bsonType': 'object',
                    'additionalProperties': False,
                    'required': ['_id', 'db-nr', 'start-address', 'data-type', 'log-interval', 'batch-interval', 'PLC_IP'],
                    'properties': {
                        '_id': {
                            'bsonType': 'objectId'
                        },
                        'db-nr': {
                            'bsonType': 'int'
                        },
                        'start-address': {
                            'bsonType': 'int'
                        },
                        'data-type': {
                            'bsonType': 'int'
                        },
                        'log-interval': {
                            'bsonType': 'int'
                        },
                        'batch-interval': {
                            'bsonType': 'int'
                        },
                        'PLC_IP': {
                            'bsonType': 'string'
                        }
                    }
                }
            })

        if selection == 2:
            #Create collection for Data and add validation to schema
            self.db.drop_collection("Data")
            self.db.create_collection('Data', validator={
                '$jsonSchema': {
                    'bsonType': 'object',
                    'additionalProperties': False,
                    'required': ['_id', 'tag_id', 'value', 'timestamp', 'published'],
                    'properties': {
                        '_id': {
                            'bsonType': 'objectId'
                        },
                        'tag_id': {
                            'bsonType': 'objectId'
                        },
                        'value': {
                           'bsonType': ['bool' , 'int' , 'double' , 'string' , 'long']
                        },
                        'timestamp': {
                            'bsonType': 'double'
                        },
                        'published': {
                            'bsonType': 'bool'
                        }
                    }
                }
            })
###########################################################
###                         PLCS                        ###
###########################################################   

    def CountPLCs(self):
        col = self.db["PLCs"]
        res = col.count_documents({})
        return res
        
    def insertPLC(self, item):
        col = self.db["PLCs"]
        try:
            if col.count_documents({"ip_address": item["ip_address"]}):
                col.replace_one({"ip_address": item["ip_address"]}, item)
            else:
                col.insert_one(item)
            return True
        except errors.BulkWriteError as e:
            print("ERROR! MongoDB.insertPLC(): Schema rules not satisfied")
            print(e.details)
            return False

    def deletePLC(self, ip_address):
        col = self.db["PLCs"]
        try:
            col.delete_one({"ip_address": ip_address})
            return True
        except errors.OperationFailure as e:
            print("Could not delete PLC!")
            print(e.details)
            return False

    def getPlcs(self):
        col = self.db["PLCs"]
        return col.find()
    
    def getAllActivePLC(self):
        col = self.db["PLCs"]
        return col.find({"active": True})

###########################################################
###                         TAGS                        ###
###########################################################
    def CountTags(self):
        col = self.db["Tags"]
        res = col.count_documents({})
        print("Updated taglist.")
        return res

    def dropAndInsertTags(self, tags):
        col = self.db["Tags"]
        col.drop()
        try:
            col.insert_many(tags)
            return True
        except errors.BulkWriteError as e:
            print("ERROR! MongoDB.InsertTags(): Schema rules not satisfied")
            print(e.details)
            return False

    def InsertTags(self, tags):
        col = self.db["Tags"]
        try:
            col.insert_many(tags)
        except errors.BulkWriteError as e:
            print("ERROR! MongoDB.InsertTags(): Schema rules not satisfied")
            print(e.details)

    def getTagsForPLC(self, PLC_IP):
        col = self.db["Tags"]
        return col.find({"PLC_IP": PLC_IP})
    
    def UpdateTags(self, tags):
        col = self.db["Tags"]
        col.drop()
        self.InsertTags(tags)

###########################################################
###                         DATA                        ###
###########################################################

    def insertDataPoint(self, tag_id, value, timestamp):
        col = self.db["Data"]        
        try:         
            col.insert_one({
                "tag_id": ObjectId(tag_id),
                "value": value,
                "timestamp": timestamp,
                "published": False
            })
        except errors.OperationFailure as e:
            print("Failed to insert datapoint!")
            print(e.details)

    def deleteDataPoints(self, ids):
        col = self.db["Data"]
        del_query = {
            "_id": {
                "$in": ids
            }
        }
        try:
            col.delete_many(del_query)
        except errors.OperationFailure as e:
            print("Failed to delete datapoint!")
            print(e.details)

    def getUnpublishedDatapoints(self):
        col = self.db["Data"]
        return col.find({"published": False})