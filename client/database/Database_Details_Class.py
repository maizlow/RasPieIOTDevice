class DataBaseDetails():
    def __init__(self, ip_address, port, name):
        self.ip = ip_address
        self.port = int(port)
        self.name = name

    def get_data(self):
        return {"IP": self.ip, "Port": self.port, "Name": self.name}

    def __repr__(self):
        return "<__main__.PLC: IP = " + str(self.ip) + "; Port = " + str(self.port) + "; Name = " + str(self.name) + ">"