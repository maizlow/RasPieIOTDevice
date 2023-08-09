import configparser
from database import Database_Details_Class

db_info = Database_Details_Class.DataBaseDetails

def getConfiguredDatabase():
    config_obj = configparser.ConfigParser()
    config_obj.read("config.ini")
    return db_info(config_obj["database"]["ip"], config_obj["database"]["port"], config_obj["database"]["name"])

def getAWSEndpoint():
    config_obj = configparser.ConfigParser()
    config_obj.read("config.ini")
    return config_obj["cloud"]["AWS_ENDPOINT"]

def getThingName():
    config_obj = configparser.ConfigParser()
    config_obj.read("config.ini")
    cloud = config_obj["cloud"]
    return cloud["THING_NAME"]

def getClientID():
    config_obj = configparser.ConfigParser()
    config_obj.read("config.ini")
    cloud = config_obj["cloud"]
    return cloud["CLIENT_ID"]

def getPublishStatus():
    config_obj = configparser.ConfigParser()
    config_obj.read("config.ini")
    cloud = config_obj["cloud"]
    if cloud["PUBLISH"] == "False":
        return False
    elif cloud["PUBLISH"] == "True":
        return True

def getMongoAuth():
    config_obj = configparser.ConfigParser()
    config_obj.read("config.ini")
    return (config_obj["database"]["username"], config_obj["database"]["password"])