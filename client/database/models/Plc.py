class Plc():
    ip : str
    rack : int
    slot : int
    active : bool
    description : str

    def __init__(self, id, ip_address, rack_nr, slot_nr, active, description):
        self.id = id
        self.ip = ip_address
        self.rack = int(rack_nr)
        self.slot = int(slot_nr)
        self.active = active
        self.description = description

    def get_data(self):
        return {"IP": self.ip, "Rack": self.rack, "Slot": self.slot, "Active": self.active}

    def __repr__(self):
        return "<__main__.PLC: IP = " + str(self.ip) + "; RACK = " + str(self.rack) + "; SLOT = " + str(self.slot) + "; ACTIVE = " + str(self.active) + ">"