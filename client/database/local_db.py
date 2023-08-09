from pymongo import MongoClient, errors, ReplaceOne
from bson import ObjectId
import config, platform
from bson.json_util import dumps, loads

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
            self.db["PLCs"].create_index("id", unique = True)
        try:
            self.db.validate_collection("Tags")      # Try to validate a collection
        except errors.OperationFailure:             # If the collection doesn't exist
            self.create_collections(1)
        try:
            self.db.validate_collection("Datatypes")      # Try to validate a collection
        except errors.OperationFailure:             # If the collection doesn't exist
            self.create_collections(2)
        try:
            self.db.validate_collection("Data")      # Try to validate a collection
        except errors.OperationFailure:             # If the collection doesn't exist
            self.create_collections(3)

    def create_collections(self, selection):
        if selection == 0:
            #Create collection for PLCs and add validation to schema
            self.db.drop_collection("PLCs")
            col = self.db.create_collection('PLCs', validator={
                '$jsonSchema': {
                    'bsonType': 'object',
                    'additionalProperties': True,
                    'required': ['id', 'ip', 'slot', 'rack', 'active', 'description'],
                    'properties': {
                        '_id': {
                            'bsonType': 'objectId'
                        },
                        'id': {
                            'bsonType': 'string'
                        },
                        'ip': {
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
                        },
                        'description': {
                            'bsonType': 'string'
                        }
                    }
                }
            })

        if selection == 1:
            #Create collection for Tags and add validation to schema
            self.db.drop_collection("Tags")
            self.db.create_collection('Tags', validator={
                '$jsonSchema': {
                    'bsonType': 'object',
                    'additionalProperties': True,
                    'required': ['id', 'dbnumber', 'startaddress', 'tagDatatypeId', 'loginterval', 'batchInterval', 'tagPlcId', 'description'],
                    'properties': {
                        '_id': {
                            'bsonType': 'objectId'
                        },
                        'id': {
                            'bsonType': 'string'
                        },
                        'dbnumber': {
                            'bsonType': 'int'
                        },
                        'startaddress': {
                            'bsonType': 'int'
                        },
                        'tagDatatypeId': {
                            'bsonType': 'string'
                        },
                        'bitnumber': {
                            'bsonType': 'int'
                        },
                        'loginterval': {
                            'bsonType': 'int'
                        },
                        'batchInterval': {
                            'bsonType': 'int'
                        },
                        'tagPlcId': {
                            'bsonType': 'string'
                        },
                        'description': {
                            'bsonType': 'string'
                        }
                    }
                }
            })
        
        if selection == 2:
            #Create collection for Datatypes and add validation to schema
            self.db.drop_collection("Datatypes")
            self.db.create_collection('Datatypes', validator={
                '$jsonSchema': {
                    'bsonType': 'object',
                    'additionalProperties': True,
                    'required': ['id', 'name', 'identifier'],
                    'properties': {
                        '_id': {
                            'bsonType': 'objectId'
                        },
                        'id': {
                            'bsonType': 'string'
                        },
                        'name': {
                           'bsonType': 'string'
                        },
                        'identifier': {
                            'bsonType': 'int'
                        }
                    }
                }
            })

        if selection == 3:
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

    def dropAndInsertPLCs(self, plcs):
        col = self.db["PLCs"]
        col.drop()
        try:
            col.insert_many(plcs["Items"])
            return True
        except errors.BulkWriteError as e:
            print("ERROR! MongoDB.dropAndInsertPLCs(): Schema rules not satisfied")
            print(e.details)
            return False

    def insertPLC(self, item):
        print("ITEM:")
        print(item)
        col = self.db["PLCs"]
        try:
            if col.count_documents({"id": item['id']}):
                col.replace_one({"id": item['id']}, item)
            else:
                col.insert_one(item)
            return True
        except errors.BulkWriteError as e:
            print("ERROR! MongoDB.insertPLC(): Schema rules not satisfied")
            print(e.details)
            return False
    
    def UpsertPLCs(self, plcs):
        col = self.db["PLCs"]
        try:            
            request = []
            for plc in plcs["Items"]:
                req = ReplaceOne({'id': plc["id"]}, plc, upsert=True)
                request.append(req)
            col.bulk_write(request)
            return True
        except errors.OperationFailure as e:
            print(e)
            return False

    def InsertPLCs(self, plcs):
        col = self.db["PLCs"]
        try:
            col.insert_many(plcs["Items"])
            return True
        except errors.BulkWriteError as e:
            print(e.code)
            print("ERROR! MongoDB.InsertPLCs(): Schema rules not satisfied")
            print(e.details)
            return False

    def deletePLC(self, ip_address):
        col = self.db["PLCs"]
        try:
            col.delete_one({"ip": ip_address})
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
        return res

    def UpsertTags(self, tags):
        col = self.db["Tags"]
        try:            
        
            request = []
            for tag in tags["Items"]:
                req = ReplaceOne({'id': tag["id"]}, tag, upsert=True)
                request.append(req)
            if(request.__len__() > 0):
                col.bulk_write(request)
            return True
        except errors.OperationFailure as e:
            print(e)
            return False

    def dropAndInsertTags(self, tags):
        col = self.db["Tags"]
        col.drop()
        try:
            col.insert_many(tags["Items"])
            return True
        except errors.BulkWriteError as e:
            print("ERROR! MongoDB.dropAndInsertTags(): Schema rules not satisfied")
            print(e.details)
            return False

    def InsertTags(self, tags):
        col = self.db["Tags"]
        try:
            col.insert_many(tags["Items"])
            return True
        except errors.BulkWriteError as e:
            print("ERROR! MongoDB.InsertTags(): Schema rules not satisfied")
            print(e.details)
            return False

    def getTagsForPLC(self, plcId):
        col = self.db["Tags"]
        return col.find({"id": plcId})
    
    def getTagsForPLC2(self, plcId):
        col = self.db["Tags"]
        tags = col.find({"id": plcId})
        result = col.aggregate([
            {
                '$match': { "tagPlcId" : plcId }
            },
            {
                '$lookup': {
                    'from': 'Datatypes',
                    'localField': 'tagDatatypeId',
                    'foreignField': 'id',
                    'as': 'datatype'
                }                
            }
        ])
        # print(dumps(list(result)))
        # print('/n')
        return list(result)


    def UpdateTags(self, tags):
        col = self.db["Tags"]
        col.drop()
        self.InsertTags(tags)

###########################################################
###                    DATATYPES                        ###
###########################################################

    def CountDatatypes(self):
        col = self.db["Datatypes"]
        res = col.count_documents({})
        return res

    def dropAndInsertDatatypes(self, datatypes):
        col = self.db["Datatypes"]
        col.drop()
        try:
            col.insert_many(datatypes["Items"])
            return True
        except errors.BulkWriteError as e:
            print("ERROR! MongoDB.dropAndInsertDatatypes(): Schema rules not satisfied")
            print(e.details)
            return False

    def getDatatype(self, id):
        col = self.db["Datatypes"]
        return col.find({"cloud_id": id})

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