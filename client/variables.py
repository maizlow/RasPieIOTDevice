import os
from pathlib import Path

cwd = Path.cwd()
rel_path1 = "../"

CERT_FILEPATH = (cwd / rel_path1 / "IOT_Demo_Certs/device.pem.crt").resolve().__str__() #"/home/abece/certs/device.pem.crt"
PRI_KEY_FILEPATH = (cwd / rel_path1 / "IOT_Demo_Certs/private.pem.key").resolve().__str__() #"/home/abece/certs/private.pem.key"
CA_FILEPATH = (cwd / rel_path1 / "IOT_Demo_Certs/AmazonRootCA1.pem").resolve().__str__() #"/home/abece/certs/AmazonRootCA1.pem"
#CLIENT_ID = "IOT_Demo" #Thing name
#ENDPOINT = os.getenv("IOT_AWS_ENDPOINT", "a2wh6cf1hx3p6x-ats.iot.eu-north-1.amazonaws.com")