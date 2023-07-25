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

def getClientID():
    config_obj = configparser.ConfigParser()
    config_obj.read("config.ini")
    return config_obj["cloud"]["CLIENT_ID"]

def getMongoAuth():
    config_obj = configparser.ConfigParser()
    config_obj.read("config.ini")
    return (config_obj["database"]["username"], config_obj["database"]["password"])